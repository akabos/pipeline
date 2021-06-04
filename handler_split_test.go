package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitHandler_Loop(t *testing.T) {

	t.Run("ok", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		inch := Sequence(ctx, 1)
		outch := make(chan Item, 2)

		f := SplitHandler(func(ctx context.Context, obj interface{}, err error) ([]interface{}, error) {
			return []interface{}{obj, obj}, err
		})
		err := f.Loop(ctx, inch, outch)
		require.NoError(t, err)

		close(outch)
		assert.Equal(t, []interface{}{Wrap(0, nil), Wrap(0, nil)}, chanToSlice(outch))
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
		defer cancel()

		inch := Sequence(context.Background(), 10)
		outch := make(chan Item, 10)

		f := SplitHandler(func(ctx context.Context, obj interface{}, err error) ([]interface{}, error) {
			cancel()
			return []interface{}{obj, obj}, err
		})
		err := f.Loop(ctx, inch, outch)
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))

		close(outch)
		assert.Less(t, len(chanToSlice(outch)), 10)
	})

	t.Run("empty output", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		inch := Sequence(ctx, 10)
		outch := make(chan Item)

		f := SplitHandler(func(ctx context.Context, obj interface{}, err error) ([]interface{}, error) {
			return nil, err
		})
		err := f.Loop(ctx, inch, outch)
		require.NoError(t, err)

		close(outch)
		assert.Len(t, chanToSlice(outch), 0)
	})
}
