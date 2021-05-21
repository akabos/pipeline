package pipeline

import (
	"context"
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
		Process: func(ctx context.Context, in []interface{}) (out []interface{}, err error) {
			out = append(out, in)
			return
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
		assert.Equal(t, expect[i], obj.([]interface{}))
	}
}

func TestBatchHandler_Wait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := BatchHandler{
		Size: 3,
		Wait: time.Millisecond * 450,
		Process: func(ctx context.Context, in []interface{}) (out []interface{}, err error) {
			out = append(out, in)
			return
		},
	}
	p := append(Pipeline{}, HandlerStage(&b, 1, 0))

	inch := make(chan interface{})
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Duration(i) * time.Millisecond * 100)
			inch <- i
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
			{5},
			{6},
			{7},
			{8},
			{9},
		}
	)
	for i := range expect {
		obj, err = Unwrap(<-outch)
		require.NoError(t, err)
		assert.Equal(t, expect[i], obj.([]interface{}))
	}


	// Output:
	//
	// [0 1 2]
	// [3 4]
	// [5]
	// [6]
	// [7]
	// [8]
	// [9]
}
