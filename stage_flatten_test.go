package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inch = make(chan Item, 3)

	for i := 0; i < 3; i++ {
		inch <- Wrap([]Item{Wrap(0, nil), Wrap(1, nil), Wrap(2, nil)}, nil)
	}

	p := append(Pipeline{}, Flatten())
	outch := p.Run(ctx, inch)

	for i := 0; i < 9; i++ {
		x, err := Unwrap(<-outch)
		require.NoError(t, err)
		assert.Equal(t, i%3, x.(int))
	}
}
