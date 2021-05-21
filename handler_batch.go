package pipeline

import (
	"context"
	"math"
	"time"
)

// BatchHandler processes inputs grouped sized batches. Receiving an error in the input, it passes it downstream
// unchanged.
type BatchHandler struct {
	// Size defines the maximum size of a batch sent into processor
	Size int

	// Wait defines max time handler waits for the batch. Once the specified time exceeds, handler will either
	// process partial batch or resets the timer if no inputs accumulated. Defaults to math.MaxInt64 nanoseconds.
	Wait time.Duration

	// Process defines callback function
	Process func(ctx context.Context, in []interface{}) (out []interface{}, err error)
}

func (b *BatchHandler) Loop(ctx context.Context, inch <-chan Item, outch chan<- Item) error {
	var (
		in  []interface{}
		out []interface{}
		err error
	)
	if b.Wait == 0 {
		b.Wait = math.MaxInt64
	}
	for {
		in, err = b.batch(ctx, inch, outch)
		if err != nil {
			return err
		}
		if len(in) == 0 {
			return nil
		}
		out, err = b.Process(ctx, in)
		if err != nil {
			err = b.out(ctx, Wrap(nil, err), outch)
			if err != nil {
				return err
			}
			continue
		}
		for i := range out {
			err = b.out(ctx, Wrap(out[i], nil), outch)
			if err != nil {
				return err
			}
		}
	}
}

func (b *BatchHandler) batch(ctx context.Context, inch <-chan Item, outch chan<- Item) ([]interface{}, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		x   Item
		obj interface{}
		err error
		ok  bool
		out = make([]interface{}, 0, b.Size)
		t   = time.NewTimer(b.Wait)
	)
	defer func() {
		if !t.Stop() {
			<-t.C
		}
	}()

	for len(out) < b.Size {
		select {
		case <-ctx.Done():
			return out, err
		case x, ok = <-inch:
			if !ok {
				// input channel closed, return accumulated batch
				return out, nil
			}
			obj, err = Unwrap(x)
			if err == nil {
				// append item to the batch
				out = append(out, obj)
			} else {
				// pass through error
				err = b.out(ctx, x, outch)
				if err != nil {
					return nil, err
				}
			}
		case <-t.C:
			t.Reset(b.Wait)
			// time expired
			if len(out) > 0 {
				// return accumulated batch if any
				return out, nil
			}
		}
	}

	return out, nil
}

func (b *BatchHandler) out(ctx context.Context, itm Item, outch chan<- Item) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outch <- itm:
		return nil
	}
}
