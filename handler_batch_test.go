package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := BatchHandler{
		Size: 3,
		Process: func(ctx context.Context, in []interface{}, err error) ([]interface{}, error) {
			out := []interface{}{in[:]}
			return out, err
		},
	}
	p := append(Pipeline{}, HandlerStage(&b, 1, 0))

	ch := p.Run(ctx, Sequence(ctx, 10))

	var (
		obj interface{}
		err error

		expect = [][]interface{}{
			{0, 1, 2},
			{3, 4, 5},
			{6, 7, 8},
			{9},
		}
	)
	for i := range expect {
		obj, err = Unwrap(<-ch)
		require.NoError(t, err)
		assert.Equal(t, expect[i], obj)
	}
}

func TestBatchHandler_Wait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := BatchHandler{
		Size: 3,
		Wait: time.Millisecond * 90,
		Process: func(ctx context.Context, in []interface{}, err error) ([]interface{}, error) {
			out := []interface{}{in}
			return out, err
		},
	}
	p := append(Pipeline{}, HandlerStage(&b, 1, 0))

	inch := make(chan Item)
	go func() {
		for i := 0; i < 10; i++ {
			switch i {
			case 5:
				time.Sleep(time.Millisecond*100)
			case 7:
				time.Sleep(time.Millisecond*100)
			}
			inch <- Wrap(i, nil)
		}
		close(inch)
	}()

	outch := p.Run(ctx, inch)

	var (
		obj interface{}
		err error

		expect = [][]interface{}{
			{0, 1, 2},
			{3, 4},
			{5, 6},
			{7, 8, 9},
		}
	)
	for i := range expect {
		x, ok := <-outch
		require.True(t, ok)
		obj, err = Unwrap(x)
		require.NoError(t, err)
		assert.Equal(t, expect[i], obj.([]interface{}))
	}
}

func TestBatchHandler_ErrPassThrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := errors.New("expected")

	p := append(Pipeline{},
		HandlerStage(SimpleHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
			c := obj.(int)
			if c%2 == 0 {
				return c, nil
			} else {
				return nil, e
			}
		}), 1, 0),
		HandlerStage(&BatchHandler{
			Size: 3,
			Process: func(ctx context.Context, in []interface{}, err error) ([]interface{}, error) {
				return in[:], err
			},
		}, 1, 0),
	)

	outch := p.Run(ctx, Sequence(ctx, 10))
	out := chanToSlice(outch)

	assert.Len(t, out, 10)
	for i := range out {
		obj, err := Unwrap(out[i].(Item))
		if obj != nil {
			assert.Equal(t, 0, obj.(int)%2)
		}
		if err != nil {
			assert.True(t, errors.Is(err, e))
		}
	}
}
