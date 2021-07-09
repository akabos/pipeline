package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnwrapChunk(t *testing.T) {
	c := []Item{Wrap(0, nil), Wrap(1, nil), Wrap(2, nil)}
	in := Wrap(c, nil)
	out, err := UnwrapChunk(in)
	require.NoError(t, err)
	assert.Equal(t, Wrap(0, nil), out[0])
	assert.Equal(t, Wrap(1, nil), out[1])
	assert.Equal(t, Wrap(2, nil), out[2])
}

func TestChunkStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := append(Pipeline{}, Chunk(3))
	ch := p.Run(ctx, Sequence(ctx, 10))

	var (
		x   interface{}
		err error

		expect = [][]Item{
			{Wrap(0, nil), Wrap(1, nil), Wrap(2, nil)},
			{Wrap(3, nil), Wrap(4, nil), Wrap(5, nil)},
			{Wrap(6, nil), Wrap(7, nil), Wrap(8, nil)},
			{Wrap(9, nil)},
		}
	)
	for i := range expect {
		x, err = UnwrapChunk(<-ch)
		require.NoError(t, err)
		assert.Equal(t, expect[i], x)
	}
}

func TestChunkStageWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := append(Pipeline{}, ChunkWait(3, time.Millisecond*90))

	inch := make(chan Item)
	go func() {
		for i := 0; i < 10; i++ {
			switch i {
			case 5:
				time.Sleep(time.Millisecond * 100)
			case 7:
				time.Sleep(time.Millisecond * 100)
			}
			inch <- Wrap(i, nil)
		}
		close(inch)
	}()

	outch := p.Run(ctx, inch)

	var (
		err    error
		chunk  []Item
		expect = [][]Item{
			{Wrap(0, nil), Wrap(1, nil), Wrap(2, nil)},
			{Wrap(3, nil), Wrap(4, nil)},
			{Wrap(5, nil), Wrap(6, nil)},
			{Wrap(7, nil), Wrap(8, nil), Wrap(9, nil)},
		}
	)
	for i := range expect {
		chunk, err = UnwrapChunk(<-outch)
		require.NoError(t, err)
		assert.Equal(t, expect[i], chunk)
	}
}
