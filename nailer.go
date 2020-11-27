package dhammer

import (
	"context"
	"io"

	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Nailer struct {
	*shutter.Shutter
	ctx          context.Context
	In           chan interface{}
	Out          chan interface{}
	decoupler    chan chan interface{}
	nailerFunc   NailerFunc
	checkIfEmpty chan bool
	discardAll   bool

	logger *zap.Logger
}

type NailerFunc func(context.Context, interface{}) (interface{}, error)
type NailerOption = func(h *Nailer)

func NailerDiscardAll() NailerOption {
	return func(n *Nailer) {
		n.discardAll = true
	}
}

func NailerLogger(logger *zap.Logger) NailerOption {
	return func(n *Nailer) {
		n.logger = logger
	}
}

func NewNailer(maxConcurrency int, nailerFunc NailerFunc, options ...NailerOption) *Nailer {
	nailer := &Nailer{
		Shutter:      shutter.New(),
		In:           make(chan interface{}, 1),
		Out:          make(chan interface{}),
		decoupler:    make(chan chan interface{}, maxConcurrency),
		checkIfEmpty: make(chan bool),
		nailerFunc:   nailerFunc,
		logger:       zlog,
	}

	for _, option := range options {
		option(nailer)
	}

	return nailer
}

func (n *Nailer) Start(ctx context.Context) {
	n.ctx = ctx
	go n.runInput()
	go n.linearizeOutput()
}

func (n *Nailer) Push(ctx context.Context, in interface{}) {
	select {
	case n.In <- in:
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

func (n *Nailer) PushAll(ctx context.Context, ins []interface{}) {
	n.Start(ctx)
	go func() {
		for _, in := range ins {
			n.In <- in
		}
		n.Close()
	}()
}

// WaitUntilEmpty waits until no more input nor active inflight operations is in progress
// blocking the current goroutine along the way. The output must be consumed for this method to
// work. You should use `NailerDiscardall()` option if you don't care about the output.
//
// **Important** You are responsible of ensuring that no new inputs are being push while waiting.
// This method does not protect against such case right now and could unblock just before a new
// input is pushed which would make the instance "non-emtpy" anymore.
func (n *Nailer) WaitUntilEmpty(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.Terminating():
			return
		case n.checkIfEmpty <- true:
			if len(n.In) == 0 && len(n.decoupler) == 0 {
				return
			}
		}
	}
}

func (n *Nailer) Drain() {
	go func() {
		for {
			<-n.Out
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

func (n *Nailer) Close() {
	close(n.In)
}

func (n *Nailer) runInput() {
	n.logger.Debug("running input consumer")
	var toProcess interface{}
	closed := false
	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug("input reader context done")
			n.Shutdown(n.ctx.Err())
			return
		case <-n.Terminating():
			n.logger.Debug("input reader shutter terminating")
			return
		case next, ok := <-n.In:
			if !ok {
				n.logger.Debug("input reader channel closed")
				closed = true
				break
			}
			toProcess = next
		}

		if traceEnabled {
			n.logger.Debug("input reader sending input, breaking loop", zap.Any("data", toProcess))
		}

		if closed {
			n.logger.Debug("input reader no more input to process and channel closed, closing decoupler")
			close(n.decoupler)
			return
		}

		var processOut chan interface{}
		if !n.discardAll {
			processOut = make(chan interface{}, 1)
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
			go n.processInput(toProcess, processOut)
		}
	}
}

func (n *Nailer) processInput(in interface{}, out chan interface{}) {
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
	case out <- output:
		if traceEnabled {
			n.logger.Debug("processed input, sent result to output channel")
		}
	}
}

func (n *Nailer) linearizeOutput() {
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

func (n *Nailer) outputSingleBatch(ch chan interface{}) error {
	if traceEnabled {
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
				if traceEnabled {
					n.logger.Debug("single batch channel received null, nothing more to process")
				}
				return nil // done
			}

			if traceEnabled {
				n.logger.Debug("output channel resolved to a value, sending it to consumer")
			}

			if err := n.safelySend(obj, n.Out); err != nil {
				return err
			}
		}
	}
}

func (n *Nailer) safelySend(obj interface{}, out chan interface{}) error {
	select {
	case <-n.ctx.Done():
		n.Shutdown(n.ctx.Err())
		return n.ctx.Err()
	case <-n.Terminating():
		return io.EOF
	case out <- obj:
		if traceEnabled {
			n.logger.Debug("forwarded element to out channel")
		}
	}
	return nil
}
