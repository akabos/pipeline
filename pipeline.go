package pipeline

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"
)

type (
	VentFunc      func(ctx context.Context, ch chan<- interface{}) error
	TransformFunc func(ctx context.Context, in interface{}) (interface{}, error)
	SinkFunc      func(ctx context.Context, ch <-chan interface{}) error

	Handler interface {
		Vent(ctx context.Context, ch chan<- interface{}) error
		Transform(ctx context.Context, in interface{}) (interface{}, error)
		Sink(ctx context.Context, ch <-chan interface{}) error
	}
)

func New() *Pipeline {
	return &Pipeline{
		concurrency: 2,
		ventBuffer:  0,
		sinkBuffer:  0,
		ventF: func(_ context.Context, _ chan<- interface{}) error {
			return nil
		},
		transformF: func(_ context.Context, in interface{}) (interface{}, error) {
			return in, nil
		},
		sinkF: func(_ context.Context, ch <-chan interface{}) error {
			for range ch {
			}
			return nil
		},
	}
}

type Pipeline struct {
	mu sync.Mutex

	concurrency uint32
	ventBuffer  uint32
	sinkBuffer  uint32

	ventF      VentFunc
	transformF TransformFunc
	sinkF      SinkFunc
}

func (p *Pipeline) WithConcurrency(n uint32) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	if n == 0 {
		panic("concurrency can't be 0")
	}

	p.concurrency = n
	return p
}

func (p *Pipeline) WithVentBuffer(n uint32) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ventBuffer = n
	return p
}

func (p *Pipeline) WithSinkBuffer(n uint32) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sinkBuffer = n
	return p
}

func (p *Pipeline) WithVentFunc(f VentFunc) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ventF = f
	return p
}

func (p *Pipeline) WithTransformFunc(f TransformFunc) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.transformF = f
	return p
}

func (p *Pipeline) WithSinkFunc(f SinkFunc) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sinkF = f
	return p
}

func (p *Pipeline) WithHandler(h Handler) *Pipeline {
	return p.WithVentFunc(h.Vent).WithTransformFunc(h.Transform).WithSinkFunc(h.Sink)
}

func (p *Pipeline) Run() error {
	return p.RunWithContext(context.Background())
}

func (p *Pipeline) RunWithContext(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ventCh := make(chan interface{}, p.ventBuffer)
	sinkCh := make(chan interface{}, p.sinkBuffer)

	g, ctx := errgroup.WithContext(ctx) // group to run vent and sink

	g.Go(p.ventFunc(ctx, ventCh))
	g.Go(p.sinkFunc(ctx, sinkCh))
	g.Go(p.workerGroupFunc(ctx, ventCh, sinkCh))

	return g.Wait()
}

func (p *Pipeline) workerGroupFunc(ctx context.Context, vent <-chan interface{}, sink chan<- interface{}) func() error {
	return func() error {
		defer close(sink)
		g, ctx := errgroup.WithContext(ctx)
		for i := uint32(0); i < p.concurrency; i++ {
			g.Go(p.workerFunc(ctx, vent, sink))
		}
		return g.Wait()
	}
}

func (p *Pipeline) workerFunc(ctx context.Context, vent <-chan interface{}, sink chan<- interface{}) func() error {
	return func() error {
		if p.transformF == nil {
			return errors.New("transform function is not set")
		}
		for in := range vent {
			out, err := p.transformF(ctx, in)
			if err != nil {
				return err
			}
			sink <- out
		}
		return nil
	}
}

func (p *Pipeline) ventFunc(ctx context.Context, ch chan interface{}) func() error {
	return func() error {
		if p.ventF == nil {
			return errors.New("vent function is not set")
		}
		defer close(ch)
		return p.ventF(ctx, ch)
	}
}

func (p *Pipeline) sinkFunc(ctx context.Context, ch <-chan interface{}) func() error {
	return func() error {
		if p.sinkF == nil {
			return errors.New("sink function is not set")
		}
		return p.sinkF(ctx, ch)
	}
}
