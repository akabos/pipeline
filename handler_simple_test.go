package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleHandler_Loop(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		inch := Sequence(ctx, 10)
		outch := make(chan Item, 10)

		f := SimpleHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
			return obj, err
		})
		err := f.Loop(ctx, inch, outch)
		require.NoError(t, err)

		close(outch)
		assert.Len(t, chanToSlice(outch), 10)
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mux := sync.Mutex{}
		mux.Lock()
		cond := sync.NewCond(&mux)

		go func() {
			cond.Wait()
			cancel()
		}()

		inch := Sequence(context.Background(), 10)
		outch := make(chan Item, 10)

		f := SimpleHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
			cond.Broadcast()
			return obj, err
		})
		err := f.Loop(ctx, inch, outch)
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))

		close(outch)
		assert.Less(t, len(chanToSlice(outch)), 10)
	})
}

func chanToSlice(ch <-chan Item) (s []interface{}) {
	for v := range ch {
		s = append(s, v)
	}
	return s
}
