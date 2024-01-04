package dhammer

import (
	"context"
	"io"

	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// We needed to come up with a non-generic struct to contain the config
// values as otherwise, we had to generify [NailerOption] and it caused
// the library consumer to adjust for it which was not ergonomic.
type nailerConfig struct {
	discardAll bool
	logger     *zap.Logger
	tracer     logging.Tracer
}

type Nailer[T any, R any] struct {
	*shutter.Shutter
	ctx            context.Context
	in             chan T
	Out            chan R
	decoupler      chan chan *R
	nailerFunc     NailerFunc[T, R]
	checkIfEmpty   chan bool
	maxConcurrency int

	discardAll bool
	logger     *zap.Logger
	tracer     logging.Tracer
}

type NailerFunc[T any, R any] func(context.Context, T) (R, error)

// NailerOption represents a configuration option that be passed to [NewNailer].
type NailerOption func(config *nailerConfig)

func NailerDiscardAll() NailerOption {
	return func(n *nailerConfig) {
		n.discardAll = true
	}
}

func NailerLogger(logger *zap.Logger) NailerOption {
	return func(n *nailerConfig) {
		n.logger = logger
	}
}

func NailerTracer(tracer logging.Tracer) NailerOption {
	return func(n *nailerConfig) {
		n.tracer = tracer
	}
}

func NewNailer[T any, R any](maxConcurrency int, nailerFunc NailerFunc[T, R], options ...NailerOption) *Nailer[T, R] {
	nailer := &Nailer[T, R]{
		Shutter:        shutter.New(),
		in:             make(chan T, 1),
		Out:            make(chan R),
		decoupler:      make(chan chan *R, 20000),
		checkIfEmpty:   make(chan bool),
		maxConcurrency: maxConcurrency,
		nailerFunc:     nailerFunc,
	}

	config := nailerConfig{
		logger: zlog,
		tracer: tracer,
	}
	for _, option := range options {
		option(&config)
	}

	nailer.discardAll = config.discardAll
	nailer.logger = config.logger
	nailer.tracer = config.tracer

	return nailer
}

// Start should be call prior sending any job to the nailer, it **must** be started
// otherwise nothing will work properly.
func (n *Nailer[T, R]) Start(ctx context.Context) {
	n.ctx = ctx
	go n.runInput()
	go n.linearizeOutput()
}

func (n *Nailer[T, R]) Push(ctx context.Context, in T) {
	select {
	case n.in <- in:
	case <-n.Terminating():
		n.logger.Debug("unable to push since nailer is terminating")
		return
	case <-ctx.Done():
		n.logger.Debug("unable to push since caller's context is done")
		return
	case <-n.ctx.Done():
		n.logger.Debug("unable to push since nailer's context is done")
		return
	}
}

// Deprecated Renamed to ExecuteAll, going to be replaced by a different implementation
// at a later time.
func (n *Nailer[T, R]) PushAll(ctx context.Context, ins []T) {
	n.ExecuteAll(ctx, ins)
}

// ExecuteAll is going to [Start] the nailer, push all `ins` into it and then call
// [Close] to stop the nailer process. It's a shortcut method if you want to execute
// pre-made jobs rapidly.
func (n *Nailer[T, R]) ExecuteAll(ctx context.Context, ins []T) {
	n.Start(ctx)
	go func() {
		for _, in := range ins {
			n.in <- in
		}
		n.Close()
	}()
}

// WaitUntilEmpty waits until no more input nor active inflight operations is in progress
// blocking along the way. This method is only useful if you need to wait until all output
// has been sent in `nailer.Out` and want to re-use the same nailer. If you simply need to
// wait until all elements has been processed, you should call `Close()` and wait until your
// last element has been processed:
//
// ```
// nailer := dhammer.NewNailer(4, func(ctx context.Context, i int) (int, error) { return i * 2, nil })
// nailer.Start()
//
// done := make(chan bool, 1)
//
//	go func() {
//		  for out := range nailer.Out {
//		    // Do something with out
//		  }
//
//		  done <- true
//		}()
//
//	for i := range []int{1, 2, 3, 4, 5, 6, 7, 8} {
//	  nailer.Push(ctx, i)
//	}
//
// nailer.Close()
// <-done
// ```
//
// **Important** It's really important to understand that 'WaitUntilEmpty' only knowns when last
// output has been sent to 'nailer.Out' channel for consumption and not **when** the output processor
// reading 'nailer.Out' as finished processing the last element of the channel! If you need to wait
// until empty the queue is empty **and** that your output processor fully processed the last item,
// you should add some waiting barrier based that is lifted once your last element has been properly
// processed.
//
// This method works only if output is consumed.
func (n *Nailer[T, R]) WaitUntilEmpty(ctx context.Context) {
	n.logger.Debug("waiting until fully empty")
	defer func() {
		n.logger.Debug("empty state reached")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.Terminating():
			return
		case n.checkIfEmpty <- true:
			if len(n.in) == 0 && len(n.decoupler) == 0 {
				return
			}
		}
	}
}

// Drain fully consumes the output channel from the nailer, discarding it right away.
func (n *Nailer[T, R]) Drain() {
	go func() {
		for range n.Out {
			// Simply consume everything out
		}
	}()

	select {
	case <-n.ctx.Done():
		n.logger.Debug("input reader context done")
		return
	case <-n.Terminating():
		n.logger.Debug("input reader shutter terminating")
		return
	case <-n.Terminated():
		n.logger.Debug("input reader shutter terminated, nothing more to process")
		return
	}
}

// Close should be called when the nailer is no more needed and closes all
// started goroutine.
func (n *Nailer[T, R]) Close() {
	n.logger.Debug("closing input channel")
	close(n.in)
}

func (n *Nailer[T, R]) runInput() {
	n.logger.Debug("running input consumer")
	var toProcess T
	closed := false
	availableWorkers := make(chan struct{}, n.maxConcurrency)
	for i := 0; i < n.maxConcurrency; i++ {
		availableWorkers <- struct{}{}
	}
	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug("input reader context done")
			n.Shutdown(n.ctx.Err())
			return
		case <-n.Terminating():
			n.logger.Debug("input reader shutter terminating")
			return
		case next, ok := <-n.in:
			if !ok {
				n.logger.Debug("input reader channel closed")
				closed = true
				break
			}
			toProcess = next
		}

		if n.tracer.Enabled() {
			n.logger.Debug("input reader sending input, breaking loop", zap.Any("data", toProcess))
		}

		if closed {
			n.logger.Debug("input reader no more input to process and channel closed, closing decoupler")
			close(n.decoupler)
			return
		}

		var processOut chan *R
		if !n.discardAll {
			processOut = make(chan *R, 1)
		}

		select {
		case <-n.ctx.Done():
			n.logger.Debug("input reader batch out context done")
			n.Shutdown(n.ctx.Err())
			return
		case <-n.Terminating():
			n.logger.Debug("input reader batch out shutter terminating")
			return
		case n.decoupler <- processOut:
			<-availableWorkers
			go func(in T, out chan *R) {
				n.processInput(in, out)
				availableWorkers <- struct{}{}
			}(toProcess, processOut)
		}
	}
}

func (n *Nailer[T, R]) processInput(in T, out chan *R) {
	defer func() {
		if out != nil {
			close(out)
		}
	}()

	output, err := n.nailerFunc(n.ctx, in)
	if err != nil {
		n.logger.Debug("nailer func returned an error, shutting down", zap.Error(err))
		n.Shutdown(err)
		return
	}

	if n.discardAll {
		return
	}

	select {
	case <-n.ctx.Done():
		n.Shutdown(n.ctx.Err())
		return
	case <-n.Terminating():
		return
	case out <- &output:
		if n.tracer.Enabled() {
			n.logger.Debug("processed input, sent result to output channel")
		}
	}
}

func (n *Nailer[T, R]) linearizeOutput() {
	n.logger.Debug("starting linearize output routine")

	defer func() {
		n.logger.Debug("linearizer terminated, closing out channel")
		close(n.Out)
	}()

	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug("linearizer context done")
			n.Shutdown(n.ctx.Err())
			return
		case <-n.Terminating():
			n.logger.Debug("linearizer shutter terminating")
			return
		case outputCh, ok := <-n.decoupler:
			if !ok {
				n.logger.Debug("linearizer decoupler channel closed, shutting down")
				n.Shutdown(nil)
				return
			}

			// We are in a discard mode, so no need to send back output to channel
			if outputCh == nil {
				break
			}

			if err := n.outputSingleBatch(outputCh); err != nil {
				n.logger.Debug("linearizer output single batch error, shutting down", zap.Error(err))
				n.Shutdown(err)
				return
			}
		case <-n.checkIfEmpty:
			// Reading from the channel wakes up active waiting go routine so it can check actual length
		}
	}
}

func (n *Nailer[T, R]) outputSingleBatch(ch chan *R) error {
	if n.tracer.Enabled() {
		n.logger.Debug("received output channel from decoupler, waiting for channel to have a value")
	}

	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug("single batch context done")
			n.Shutdown(n.ctx.Err())
			return n.ctx.Err()
		case <-n.Terminating():
			n.logger.Debug("single batch shutter terminating")
			return io.EOF
		case obj := <-ch:
			if obj == nil {
				if n.tracer.Enabled() {
					n.logger.Debug("single batch channel received null, nothing more to process")
				}
				return nil // done
			}

			if n.tracer.Enabled() {
				n.logger.Debug("output channel resolved to a value, sending it to consumer")
			}

			if err := n.safelySend(*obj, n.Out); err != nil {
				return err
			}
		}
	}
}

func (n *Nailer[T, R]) safelySend(obj R, out chan R) error {
	select {
	case <-n.ctx.Done():
		n.Shutdown(n.ctx.Err())
		return n.ctx.Err()
	case <-n.Terminating():
		return io.EOF
	case out <- obj:
		if n.tracer.Enabled() {
			n.logger.Debug("forwarded element to out channel")
		}
	}
	return nil
}

// func isZero[T any](v T) bool {
// 	return reflect.ValueOf(&v).Elem().IsZero()
// }
