package pipeline

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExamplePipeline_Run() {
	// Output:

	p := New().
		WithConcurrency(10).
		WithVentBuffer(10).
		WithVentFunc(func(ctx context.Context, ch chan<- interface{}) error {
			for i := 0; i < 100; i++ {
				ch <- i
			}
			log.Println("vent completed")
			return nil
		}).
		WithTransformFunc(func(ctx context.Context, x interface{}) (interface{}, error) {
			i, ok := x.(int)
			if !ok {
				panic("invalid type in vent chan")
			}
			req, err := http.NewRequest(http.MethodGet, "http://httpbin.org/get?page=" + strconv.Itoa(i), nil)
			if err != nil {
				return nil, err
			}
			res, err := http.DefaultClient.Do(req.WithContext(ctx))
			if err != nil {
				return nil, err
			}
			res.Body.Close()

			log.Printf("request %d completed\n", i)
			return res, nil
		}).
		WithSinkFunc(func(ctx context.Context, ch <-chan interface{}) error {
			for x := range ch {
				res, ok := x.(*http.Response)
				if !ok {
					panic("invalid type in sink chan")
				}
				log.Printf("response %s: %d\n", res.Request.URL.Query().Get("page"), res.StatusCode)
			}
			log.Println("sink completed")
			return nil
		})
	if err := p.Run(); err != nil {
		panic("pipe failed")
	}
}

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
