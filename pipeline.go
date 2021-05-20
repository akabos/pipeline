package pipeline

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

type Item interface {
	Obj() interface{}
	Err() error
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

type Stage func(ctx context.Context, inch <-chan Item) (outch <-chan Item)

type Handler func(ctx context.Context, obj interface{}, err error) (interface{}, error)

type Pipeline []Stage

func (p Pipeline) Run(ctx context.Context, inch <-chan interface{}) (outch <-chan Item) {
	outch = p.generator(ctx, inch)
	for _, stage := range p {
		outch = stage(ctx, outch)
	}
	return outch
}

func (p Pipeline) generator(ctx context.Context, inch <-chan interface{}) chan Item {
	outch := make(chan Item)
	go func() {
		for obj := range inch {
			select {
			case <-ctx.Done():
				return
			case outch <- &item{obj, nil}:
				continue
			}
		}
		close(outch)
	}()
	return outch
}

func (p Pipeline) Append(s Stage) Pipeline {
	return append(p, s)
}

// AppendHandler appends a new stage to the pipeline. The stage is generated from Handler function with specified
// concurrency and output buffer length. Output buffer effectively becomes a backlog for the next stage if there is one.
func (p Pipeline) AppendHandler(f Handler, c, b int) Pipeline {
	return p.Append(stageFromHandler(f, c, b))
}

func stageFromHandler(f Handler, c, b int) Stage {
	return func(ctx context.Context, inch <-chan Item) <-chan Item {
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		outch := make(chan Item, b)
		for i := 0; i < c; i++ {
			g.Go(func() error {
				defer cancel()
				return loop(ctx, f, inch, outch)
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

func loop(ctx context.Context, f Handler, inch <-chan Item, outch chan<- Item) error {
	var (
		obj interface{}
		err error
	)
	for in := range inch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			obj, err = f(ctx, in.Obj(), in.Err())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outch <- &item{obj, err}:
			continue
		}
	}
	return nil
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
