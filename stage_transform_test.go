package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransform(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := Transform(func(ctx context.Context, item Item) (Item, error) {
		return item, nil
	}, 1)
	p := append(Pipeline{}, s)

	outch := p.Run(ctx, Sequence(ctx, 10))
	for i := 0; i < 10; i++ {
		x, err := Unwrap(<-outch)
		require.NoError(t, err)
		assert.Equal(t, i, x.(int))
	}
}
