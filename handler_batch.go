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
			// input channel exhausted and closed; it's ok, just return without error
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
	var (
		itm Item
		obj interface{}
		err error
		out = make([]interface{}, 0, b.Size)
		t   = time.NewTimer(b.Wait)
	)
	defer t.Stop()
out:
	for len(out) < b.Size {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case itm = <-inch:
			if itm == nil {
				break out
			}
			obj, err = Unwrap(itm)
			if err != nil {
				err = b.out(ctx, itm, outch)
				if err != nil {
					return nil, err
				}
			}
			out = append(out, obj)
		case <-t.C:
			if len(out) > 0 {
				break out
			}
		}
		t.Reset(b.Wait)
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
