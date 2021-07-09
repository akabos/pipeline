package pipeline

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

type TransformFunc func(ctx context.Context, item Item) (Item, error)

// Transform returns a new stage executing concurrent 1:1 transformation
func Transform(f TransformFunc, fanout int) Stage {
	return transform(f, fanout, 0)
}

// TransformBuffered returns a new stage executing concurrent 1:1 transformation with output buffering
func TransformBuffered(f TransformFunc, fanout, buffer int) Stage {
	return transform(f, fanout, buffer)
}

func transform(f TransformFunc, fanout, buffer int) Stage {
	return func(ctx context.Context, inch <-chan Item) <-chan Item {
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		outch := make(chan Item, buffer)
		for i := 0; i < fanout; i++ {
			g.Go(func() error {
				defer cancel()
				return transformDo(ctx, inch, outch, f)
			})
		}
		go func() {
			err := g.Wait()
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					panic(err)
				}
			}
			close(outch)
			cancel()
		}()
		return outch
	}
}

func transformDo(ctx context.Context, inch <-chan Item, outch chan<- Item, f TransformFunc) error {
	var (
		out Item
		err error
	)
	for in := range inch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			out, err = f(ctx, in)
			if err != nil {
				return err
			}
			if out == nil {
				continue
			}
			err = send(ctx, outch, out)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
