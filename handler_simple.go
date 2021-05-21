package pipeline

import "context"

// SimpleHandler is a simple 1:1 handler returning exactly one output for each input
type SimpleHandler func(ctx context.Context, obj interface{}, err error) (interface{}, error)

func (f SimpleHandler) Loop(ctx context.Context, inch <-chan Item, outch chan<- Item) error {
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
