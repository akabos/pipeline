package pipeline

import (
	"context"
	"io"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"testing"
)

func ExamplePipeline() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 12

	p := Pipeline{}
	p = p.AppendHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		i := obj.(int)
		req, err := http.NewRequest(http.MethodGet, "https://httpbin.org/get?page="+strconv.Itoa(i), nil)
		if err != nil {
			return nil, err
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		log.Printf("request %d completed\n", i)
		return res, nil
	}, 3, 0)
	p = p.AppendHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		res := obj.(*http.Response)
		log.Printf("response %s: %d\n", res.Request.URL.Query().Get("page"), res.StatusCode)
		return nil, nil
	}, 1, count) // add buffer to prevent deadlock

	tasks := make(chan interface{})
	results := p.Run(ctx, tasks)

	for i := 0; i < count; i++ {
		tasks <- i
	}
	close(tasks)

	err := Drain(ctx, results)
	if err != nil {
		panic(err)
	}

	// Output:
}

func BenchmarkPipeline(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := Pipeline{}
	p = p.AppendHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
		return obj, err
	}, runtime.NumCPU(), b.N)

	inch := make(chan interface{})
	outch := p.Run(ctx, inch)

	for i := 0; i < b.N; i++ {
		inch <- i
	}
	close(inch)

	err := Drain(ctx, outch)
	if err != nil {
		b.Fatal(err)
	}
}
