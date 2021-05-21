package pipeline

import "context"

// SplitHandler is a 1:n handler returning n results for each input
type SplitHandler func(ctx context.Context, obj interface{}, err error) ([]interface{}, error)

func (f SplitHandler) Loop(ctx context.Context, inch <-chan Item, outch chan<- Item) error {
	var (
		obj []interface{}
		err error
	)
	for in := range inch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			obj, err = f(ctx, in.Obj(), in.Err())
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case outch <- Wrap(nil, err):
				continue
			}
		}
		for i := range obj {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case outch <- Wrap(obj[i], nil):
				continue
			}
		}
	}
	return nil
}
