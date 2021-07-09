package pipeline

import (
	"context"
	"math"
	"time"
)

// Chunk returns a new pipeline stage which groups incoming items into sized chunks.
func Chunk(size int) Stage {
	return chunk(size, math.MaxInt64)
}

// ChunkWait returns a new stage which groups incoming items into sized chunks. Non-empty partial chunks will be
// returned after the timeout.
func ChunkWait(size int, d time.Duration) Stage {
	return chunk(size, d)
}

func chunk(size int, d time.Duration) Stage {
	return func(ctx context.Context, inch <-chan Item) <-chan Item {
		outch := make(chan Item)
		go chunkDo(ctx, inch, outch, size, d)
		return outch
	}
}

func chunkDo(ctx context.Context, inch <-chan Item, outch chan<- Item, size int, d time.Duration) {
	defer close(outch)
	for {
		b, err := chunkAccumulate(ctx, size, d, inch)
		if err != nil {
			return
		}
		if len(b) == 0 { // edge case: input channel closed, batch empty
			return
		}
		select {
		case <-ctx.Done():
			return
		case outch <- Wrap(b, nil):
			continue
		}
	}
}

func chunkAccumulate(ctx context.Context, size int, d time.Duration, inch <-chan Item) ([]Item, error) {
	var (
		x   Item
		ok  bool
		err error
		t   = time.NewTimer(d)
		b   = make([]Item, 0, size)
	)
loop:
	for len(b) < size {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		case x, ok = <-inch:
			if !ok {
				break loop
			}
			b = append(b, x)
		case <-t.C:
			t.Reset(d)
			if len(b) > 0 {
				break loop
			}
		}
	}
	if !t.Stop() {
		<-t.C
	}
	return b, err
}

func UnwrapChunk(item Item) ([]Item, error) {
	x, err := Unwrap(item)
	if x == nil {
		return nil, err
	}
	return x.([]Item), err
}
