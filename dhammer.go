// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dhammer

import (
	"context"
	"io"

	"github.com/dfuse-io/shutter"
)

// NewHammer returns a single-use batcher
// startSingle will force batcher to run the first batch with a single object in it
func NewHammer(batchSize, maxConcurrency int, hammerFunc HammerFunc, options ...HammerOption) *Hammer {

	h := &Hammer{
		Shutter:    shutter.New(),
		In:         make(chan interface{}, batchSize),
		Out:        make(chan interface{}),
		decoupler:  make(chan chan interface{}, maxConcurrency),
		hammerFunc: hammerFunc,
		batchSize:  batchSize,
	}

	for _, option := range options {
		option(h)
	}

	return h
}

// Hammer is a tool that batches and parallelize tasks from the 'In'
// channel and writes results to the 'Out' channel. It can optimize
// performance in two ways:
//   1. It calls your HammerFunc with a maximum of `batchSize` values
//      taken from the 'In' channel (batching)
//   2. It calls your HammerFunc a maximum of `maxConcurrency`
//      times in parallel (debouncing)
//
// Both approaches give good results, but combining them gives greatest
// results, especially with large batch size with small debouncing.
//
// Closing the context will shutdown the batcher immediately.
// calling "Close" will close the `In` chan and finish processing
// until the Hammer closes the `Out` chan and shuts down
type Hammer struct {
	*shutter.Shutter
	In                chan interface{}
	Out               chan interface{}
	decoupler         chan chan interface{}
	hammerFunc        HammerFunc
	firstBatchUnitary bool
	batchSize         int
}

type HammerFunc func(context.Context, []interface{}) ([]interface{}, error)

type HammerOption = func(h *Hammer)

func FirstBatchUnitary() HammerOption {
	return func(h *Hammer) {
		h.firstBatchUnitary = true
	}
}
func SetInChanSize(size int) HammerOption {
	return func(h *Hammer) {
		h.In = make(chan interface{}, size)
	}
}

func (h *Hammer) Start(ctx context.Context) {
	go h.runInput(ctx)
	go h.linearizeOutput(ctx)
}

func (h *Hammer) Close() {
	close(h.In)
}

func (h *Hammer) runInput(ctx context.Context) {
	sendImmediately := h.firstBatchUnitary
	for {
		var inflight []interface{}
		closed := false
		for {
			select {
			case <-ctx.Done():
				h.Shutdown(ctx.Err())
				return
			case <-h.Terminating():
				return
			case next, ok := <-h.In:
				if !ok {
					closed = true
					break
				}
				inflight = append(inflight, next)
			}
			if len(h.In) < 1 || len(inflight) >= h.batchSize {
				break
			}
			if sendImmediately {
				sendImmediately = false
				break
			}
		}
		if len(inflight) == 0 && closed {
			close(h.decoupler)
			return
		}

		batchOut := make(chan interface{}, len(inflight))
		select {
		case <-ctx.Done():
			h.Shutdown(ctx.Err())
			return
		case <-h.Terminating():
			return
		case h.decoupler <- batchOut:
			go h.processBatch(ctx, inflight, batchOut)
		}
		if closed {
			close(h.decoupler)
			return
		}
	}
}

func (h *Hammer) processBatch(ctx context.Context, inflight []interface{}, out chan interface{}) {
	defer close(out)
	outputs, err := h.hammerFunc(ctx, inflight)
	if err != nil {
		h.Shutdown(err)
	}
	for _, obj := range outputs {
		if err := h.safelySend(ctx, obj, out); err != nil {
			return
		}
	}
}

func (h *Hammer) safelySend(ctx context.Context, obj interface{}, out chan interface{}) error {
	select {
	case <-ctx.Done():
		h.Shutdown(ctx.Err())
		return ctx.Err()
	case <-h.Terminating():
		return io.EOF
	case out <- obj:
	}
	return nil
}

func (h *Hammer) linearizeOutput(ctx context.Context) {
	defer close(h.Out)

	for {
		select {
		case <-ctx.Done():
			h.Shutdown(ctx.Err())
			return
		case <-h.Terminating():
			return
		case ch, ok := <-h.decoupler:
			if !ok {
				h.Shutdown(nil)
				return
			}
			if err := h.outputSingleBatch(ctx, ch); err != nil {
				h.Shutdown(err)
				return
			}
		}
	}
}

func (h *Hammer) outputSingleBatch(ctx context.Context, ch chan interface{}) error {
	for {
		select {
		case <-ctx.Done():
			h.Shutdown(ctx.Err())
			return ctx.Err()
		case <-h.Terminating():
			return io.EOF
		case obj := <-ch:
			if obj == nil {
				return nil // done
			}
			if err := h.safelySend(ctx, obj, h.Out); err != nil {
				return err
			}
		}
	}
}
