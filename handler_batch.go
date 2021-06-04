package pipeline

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/hashicorp/go-multierror"
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
	Process func(context.Context, []interface{}, error) ([]interface{}, error)
}

func (b *BatchHandler) Loop(ctx context.Context, inch <-chan Item, outch chan<- Item) error {
	var (
		batch []Item
		obj   interface{}
		err   error
		errs  *multierror.Error

		outObj []interface{}
		outErr error
	)
	if b.Wait == 0 {
		b.Wait = math.MaxInt64
	}
	for {
		batch, err = b.batch(ctx, inch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		var (
			inObj = make([]interface{}, 0, b.Size)
			inErr error
		)
		for i := range batch {
			obj, err = Unwrap(batch[i])
			if obj != nil {
				inObj = append(inObj, obj)
			}
			if err != nil {
				inErr = multierror.Append(inErr, err)
			}
		}

		if len(inObj) > 0 {
			outObj, outErr = b.Process(ctx, inObj, inErr)
			for i := range outObj {
				err = b.out(ctx, Wrap(outObj[i], nil), outch)
				if err != nil {
					return err
				}
			}
		} else {
			outErr = inErr
		}

		if outErr != nil {
			if ok := errors.As(outErr, &errs); ok {
				for i := range errs.Errors {
					err = b.out(ctx, Wrap(nil, errs.Errors[i]), outch)
					if err != nil {
						return err
					}
				}
			} else {
				err = b.out(ctx, Wrap(nil, outErr), outch)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (b *BatchHandler) batch(ctx context.Context, inch <-chan Item) (out []Item, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		x  Item
		ok bool
		t  = time.NewTimer(b.Wait)
	)
	defer func() {
		if !t.Stop() {
			<-t.C
		}
	}()

	out = make([]Item, 0, b.Size)

loop:
	for len(out) < b.Size {
		select {
		case <-ctx.Done():
			break loop
		case x, ok = <-inch:
			if !ok {
				// input channel closed, return accumulated batch
				break loop
			}
			out = append(out, x)
		case <-t.C:
			t.Reset(b.Wait)
			// time expired
			if len(out) > 0 {
				break loop
			}
		}
	}
	return
}

func (b *BatchHandler) out(ctx context.Context, itm Item, outch chan<- Item) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outch <- itm:
		return nil
	}
}
