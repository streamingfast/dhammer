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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func setBatchSizeIfBigger(biggestBatch *atomic.Int32, cnt int32) {
	for {
		bbs := biggestBatch.Load()
		if cnt > bbs {
			swapped := biggestBatch.CAS(bbs, cnt)
			if swapped {
				break
			}
		} else {
			break
		}
	}
}

func passThroughFactory(counter *atomic.Int32, biggestBatch *atomic.Int32) HammerFunc {
	return func(_ context.Context, i []interface{}) ([]interface{}, error) {
		counter.Inc()
		setBatchSizeIfBigger(biggestBatch, int32(len(i)))
		return i, nil
	}
}

func passSlowFactory(counter *atomic.Int32, biggestBatch *atomic.Int32) HammerFunc {
	return func(_ context.Context, i []interface{}) ([]interface{}, error) {
		counter.Inc()
		setBatchSizeIfBigger(biggestBatch, int32(len(i)))
		time.Sleep(2 * time.Millisecond)
		return i, nil
	}
}

func passRandomFactory(counter *atomic.Int32, biggestBatch *atomic.Int32) HammerFunc {
	return func(_ context.Context, i []interface{}) ([]interface{}, error) {
		counter.Inc()
		setBatchSizeIfBigger(biggestBatch, int32(len(i)))
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		return i, nil
	}
}

func Test_Order(t *testing.T) {

	testCases := []struct {
		name                            string
		input                           []int
		batchSize                       int
		maxConcurrency                  int
		fnc                             func(counter *atomic.Int32, biggestBatch *atomic.Int32) HammerFunc
		expectedBatchCountBetween       []int32
		expectedBiggestBatchSizeBetween []int32
		expectedTimeout                 bool
		timeoutValue                    time.Duration
	}{
		{
			name:                            "is_in_batch",
			input:                           testSequential100,
			fnc:                             passThroughFactory,
			batchSize:                       5,
			maxConcurrency:                  1,
			expectedBatchCountBetween:       []int32{20, 60},
			expectedBiggestBatchSizeBetween: []int32{2, 5},
			timeoutValue:                    time.Millisecond * 10,
		},
		{
			name:                            "is_not_in_batch",
			input:                           testSequential100,
			fnc:                             passThroughFactory,
			batchSize:                       1,
			maxConcurrency:                  5,
			expectedBatchCountBetween:       []int32{100, 100},
			expectedBiggestBatchSizeBetween: []int32{1, 1},
			timeoutValue:                    time.Millisecond * 10,
		},

		{
			name:                            "is_concurrent",
			input:                           testSequential100,
			fnc:                             passSlowFactory, // 2ms*100 > 10ms timeout
			expectedTimeout:                 false,
			batchSize:                       1,
			maxConcurrency:                  100,
			expectedBatchCountBetween:       []int32{100, 100},
			expectedBiggestBatchSizeBetween: []int32{1, 1},
			timeoutValue:                    time.Millisecond * 10,
		},
		{
			name:            "is_not_concurrent",
			input:           []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			fnc:             passSlowFactory, // 2ms * 10 > 10ms
			batchSize:       1,
			maxConcurrency:  1,
			expectedTimeout: true,
			timeoutValue:    time.Millisecond * 10,
		},
		{
			name:           "stays_ordered_with_random_processing_time",
			input:          append(testSequential100, testSequential100...),
			fnc:            passRandomFactory,
			batchSize:      3,
			maxConcurrency: 20,
			timeoutValue:   time.Second,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			batchCount := atomic.NewInt32(0)
			biggestBatchSize := atomic.NewInt32(0)

			fnc := test.fnc(batchCount, biggestBatchSize)

			h := NewHammer(test.batchSize, test.maxConcurrency, fnc)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h.Start(ctx)
			go func() {
				for _, val := range test.input {
					select {
					case h.In <- val:
					case <-h.Terminating():
						return
					}
				}
				h.Close()
			}()

			done := make(chan []int)
			go func() {
				var out []int
				for val := range h.Out {
					out = append(out, val.(int))
				}
				done <- out
			}()

			select {
			case <-time.After(test.timeoutValue):
				if test.expectedTimeout {
					<-done
					return
				}
				t.Error("test timed out")
				return
			case output := <-done:
				if test.expectedTimeout {
					t.Errorf("test should have timed out")
					return
				}
				assert.Equal(t, test.input, output)
			}

			switch len(test.expectedBatchCountBetween) {
			case 0:
				break
			case 2:
				assert.GreaterOrEqual(t, batchCount.Load(), test.expectedBatchCountBetween[0])
				assert.LessOrEqual(t, batchCount.Load(), test.expectedBatchCountBetween[1])
			default:
				t.Fatal("invalid value for test.expectedBatchCountBetween")
			}

			switch len(test.expectedBiggestBatchSizeBetween) {
			case 0:
				break
			case 2:
				assert.GreaterOrEqual(t, biggestBatchSize.Load(), test.expectedBiggestBatchSizeBetween[0])
				assert.LessOrEqual(t, biggestBatchSize.Load(), test.expectedBiggestBatchSizeBetween[1])
			default:
				t.Fatal("invalid value for test.expectedBiggestBatchSizeBetween")
			}

		})
	}
}

func Test_LargerInChanSize(t *testing.T) {
	batchCount := atomic.NewInt32(0)
	biggestBatchSize := atomic.NewInt32(0)

	h := NewHammer(1, 250, passSlowFactory(batchCount, biggestBatchSize), SetInChanSize(1000))
	ctx := context.Background()

	h.Start(ctx)

	done := make(chan bool, 1)
	go func() {
		for range h.Out {
			// Nothing to process, just consume output
		}
		done <- true
	}()

	aft := time.After(10 * time.Millisecond)
	for i := 0; i < 1000; i++ {
		select {
		case h.In <- i:
		case <-aft:
			t.Error("timed out while inserting jobs into hammer")
			return
		}
	}

	h.Close()

	select {
	case <-done:
		assert.Equal(t, int32(1), biggestBatchSize.Load())
	case <-time.After(1 * time.Second):
		t.Error("timed out while consuming hammer output")
		return
	}
}
