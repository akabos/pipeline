package pipeline

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

type Pipeline []Stage

func (p Pipeline) Run(ctx context.Context, inch <-chan Item) (outch <-chan Item) {
	outch = inch
	for _, stage := range p {
		outch = stage(ctx, outch)
	}
	return outch
}

func (p Pipeline) Append(s Stage) Pipeline {
	return append(p, s)
}

func (p Pipeline) AppendHandler(f Handler, fanout, buffer int) Pipeline {
	return p.Append(HandlerStage(f, fanout, buffer))
}

// Stage is a generic stage operating on channel level
type Stage func(ctx context.Context, inch <-chan Item) (outch <-chan Item)

type Handler interface {
	Loop(context.Context, <-chan Item, chan<- Item) error
}

func HandlerStage(f Handler, fanout, buffer int) Stage {
	return func(ctx context.Context, inch <-chan Item) <-chan Item {
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		outch := make(chan Item, buffer)
		for i := 0; i < fanout; i++ {
			g.Go(func() error {
				defer cancel()
				return f.Loop(ctx, inch, outch)
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

type Item interface {
	Obj() interface{}
	Err() error
}

type item struct {
	obj interface{}
	err error
}

func (x *item) Obj() interface{} {
	return x.obj
}

func (x *item) Err() error {
	return x.err
}

func Wrap(obj interface{}, err error) Item {
	return &item{obj, err}
}

func Unwrap(item Item) (interface{}, error) {
	return item.Obj(), item.Err()
}

func Drain(ctx context.Context, ch <-chan Item) error {
	for x := range ch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, err := Unwrap(x)
		if err != nil {
			return err
		}
	}
	return nil
}

// Sequence returns channel fed with [0...n) sequence, if n <= 0 the sequence will be infinite.
func Sequence(ctx context.Context, n int) <-chan Item {
	ch := make(chan Item)
	go func() {
	out:
		for i := 0; n < 1 || i < n; i++ {
			select {
			case <-ctx.Done():
				break out
			case ch <- Wrap(i, nil):
				continue
			}
		}
		close(ch)
		return
	}()
	return ch
}
