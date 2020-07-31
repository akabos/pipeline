package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errExpected = errors.New("expected error")

func TestPipeline_Run(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		err := New().Run()
		require.NoError(t, err)
	})

	t.Run("vent", func(t *testing.T) {
		err := New().
			WithVentFunc(
				func(ctx context.Context, vent chan<- interface{}) error {
					for i := 0; i < 1000; i++ {
						vent <- i
					}
					return nil
				},
			).
			Run()
		require.NoError(t, err)
	})

	t.Run("vent err", func(t *testing.T) {
		err := New().
			WithVentFunc(
				func(ctx context.Context, vent chan<- interface{}) error {
					return errExpected
				},
			).
			Run()
		require.Error(t, err)
		require.Exactly(t, errExpected, err)
	})

	t.Run("sink", func(t *testing.T) {
		err := New().
			WithConcurrency(1).
			WithVentFunc(
				func(ctx context.Context, vent chan<- interface{}) error {
					for i := 0; i < 1000; i++ {
						vent <- i
					}
					return nil
				},
			).
			WithSinkFunc(
				func(ctx context.Context, sink <-chan interface{}) error {
					i := 0
					for x := range sink {
						j := x.(int)
						assert.Equal(t, i, j)
						i++
					}
					return nil
				},
			).
			Run()
		require.NoError(t, err)
	})

	t.Run("sink err", func(t *testing.T) {
		err := New().
			WithVentFunc(
				func(ctx context.Context, vent chan<- interface{}) error {
					vent <- nil
					return nil
				},
			).
			WithSinkFunc(
				func(ctx context.Context, sink <-chan interface{}) error {
					<-sink
					return errExpected
				},
			).Run()
		require.Error(t, err)
		require.Exactly(t, errExpected, err)
	})

	t.Run("transform", func(t *testing.T) {
		j := 0
		err := New().
			WithConcurrency(1).
			WithVentFunc(
				func(ctx context.Context, vent chan<- interface{}) error {
					for i := 0; i < 1000; i++ {
						vent <- i
					}
					return nil
				},
			).
			WithTransformFunc(
				func(ctx context.Context, x interface{}) (interface{}, error) {
					i := x.(int)
					assert.Equal(t, j, i)
					j++
					return i, nil
				},
			).Run()

		require.NoError(t, err)

	})

	t.Run("transform err", func(t *testing.T) {
		err := New().
			WithVentFunc(
				func(ctx context.Context, vent chan<- interface{}) error {
					vent <- nil
					return nil
				},
			).
			WithTransformFunc(
				func(ctx context.Context, in interface{}) (interface{}, error) {
					return nil, errExpected
				},
			).Run()

		require.Error(t, err)
		require.Exactly(t, errExpected, err)
	})

}
