package pipeline

import (
	"context"
)

type Pipeline []Stage

func (p Pipeline) Run(ctx context.Context, inch <-chan Item) (outch <-chan Item) {
	outch = inch
	for _, stage := range p {
		outch = stage(ctx, outch)
	}
	return outch
}

// Stage is a generic stage operating on channel level
type Stage func(ctx context.Context, inch <-chan Item) (outch <-chan Item)

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
		defer close(ch)
		for i := 0; n < 1 || i < n; i++ {
			err := send(ctx, ch, Wrap(i, nil))
			if err != nil {
				return
			}
		}
	}()
	return ch
}

func send(ctx context.Context, ch chan<- Item, item Item) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- item:
		return nil
	}
}