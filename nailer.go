package dhammer

import (
	"context"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
	"io"
)

type Nailer struct {
	*shutter.Shutter
	ctx context.Context
	In                chan interface{}
	Out               chan interface{}
	decoupler         chan chan interface{}
	nailerFunc        NailerFunc
	logger *zap.Logger
}

type NailerFunc func(context.Context, interface{}) (interface{}, error)
type NailerOption = func(h *Nailer)


func NewNailer(maxConcurrency int, nailerFunc NailerFunc, logger *zap.Logger) *Nailer {
	return &Nailer{
		Shutter:    shutter.New(),
		In:         make(chan interface{}, 1),
		Out:        make(chan interface{}),
		decoupler:  make(chan chan interface{}, maxConcurrency),
		nailerFunc: nailerFunc,
		logger: logger,
	}
}

func (n *Nailer) Start(ctx context.Context) {
	n.ctx = ctx
	go n.runInput()
	go n.linearizeOutput()
}

func (n *Nailer) PushAll(ctx context.Context, out []interface{}) {
	n.Start(ctx)
	go func() {
		for _, o := range out {
			n.In <- o
		}
		n.Close()
	}()
}


func (n *Nailer) Drain() {
	go func() {
		for {
			<- n.Out
		}
	}()
	select {
		case <-n.ctx.Done():
			n.logger.Debug("input reader context done")
			return
		case <-n.Terminating():
			n.logger.Debug("input reader shutter terminating")
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
			n.logger.Debug("input reader sending  input, breaking loop",
				zap.Any("data", toProcess),
			)
		}

		if (closed) {
			n.logger.Debug("input reader no more input to process and channel closed, closing decoupler")
			close(n.decoupler)
			return
		}

		processOut := make(chan interface{}, 1)
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

	return
}

func (n *Nailer) processInput(in interface{}, out chan interface{}) {
	defer close(out)
	output, err := n.nailerFunc(n.ctx, in)
	if err != nil {
		n.Shutdown(err)
	}

	select {
	case <-n.ctx.Done():
		n.Shutdown(n.ctx.Err())
		return
	case <-n.Terminating():
		return
	case out <- output:
	}
}

func (n *Nailer) linearizeOutput() {
	defer func() {
		n.logger.Debug("linearizer terminated, closing out channel")
		close(n.Out)
	}()

	for {
		n.logger.Debug("")
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
			if err := n.outputSingleBatch(outputCh); err != nil {
				n.logger.Debug("linearizer output single batch error, shutting down", zap.Error(err))
				n.Shutdown(err)
				return
			}
		}
	}
}


func (n *Nailer) outputSingleBatch(ch chan interface{}) error {
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
	}
	return nil
}
