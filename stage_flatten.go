package pipeline

import "context"

// Flatten returns a new stage which splits incoming item chunks into individual items
func Flatten() Stage {
	return func(ctx context.Context, inch <-chan Item) <-chan Item {
		outch := make(chan Item)
		go flattenDo(ctx, inch, outch)
		return outch
	}
}

func flattenDo(ctx context.Context, inch <-chan Item, outch chan<- Item) {
	var (
		x  Item
		ok bool
	)
	defer close(outch)
	for {
		select {
		case <-ctx.Done():
			return
		case x, ok = <-inch:
			if !ok {
				return
			}
			items, err := UnwrapChunk(x)
			if err != nil {
				err = send(ctx, outch, x)
				if err != nil {
					return
				}
			}
			for i := 0; i < len(items); i++ {
				err = send(ctx, outch, items[i])
				if err != nil {
					return
				}
			}
		}
	}
}
