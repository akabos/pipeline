package pipeline

import (
	"context"
	"flag"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
)

var (
	srv *httptest.Server
	log *stdlog.Logger
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
		log = stdlog.New(os.Stderr, "", stdlog.Flags())
	} else {
		log = stdlog.New(io.Discard, "", stdlog.Flags())
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/get", func(rw http.ResponseWriter, rq *http.Request) {
		rw.WriteHeader(http.StatusNoContent)
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	m.Run()
}

func ExamplePipeline() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 20

	f1 := SimpleHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		i := obj.(int)
		req, err := http.NewRequest(http.MethodGet, srv.URL + "/get", nil)
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
	})

	f2 := SimpleHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		res := obj.(*http.Response)
		log.Printf("response %s: %d\n", res.Request.URL.Query().Get("page"), res.StatusCode)
		return nil, nil
	})

	p := Pipeline{}
	p = p.AppendHandler(f1, 3, 0)
	p = p.AppendHandler(f2, 1, count) // add buffer to prevent deadlock

	results := p.Run(ctx, Sequence(ctx, count))

	err := Drain(ctx, results)
	if err != nil {
		panic(err)
	}

	// Output:
}

func BenchmarkPipeline(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := SimpleHandler(func(ctx context.Context, obj interface{}, err error) (interface{}, error) {
		return obj, err
	})

	p := Pipeline{}
	p = p.AppendHandler(f, runtime.NumCPU(), b.N)

	outch := p.Run(ctx, Sequence(ctx, b.N))

	err := Drain(ctx, outch)
	if err != nil {
		b.Fatal(err)
	}
}
