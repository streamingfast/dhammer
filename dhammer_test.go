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

var batchCount = atomic.NewInt32(0)
var biggestBatchSize = atomic.NewInt32(0)

func setBatchSizeIfBigger(cnt int32) {
	for {
		bbs := biggestBatchSize.Load()
		if cnt > bbs {
			swapped := biggestBatchSize.CAS(bbs, cnt)
			if swapped {
				break
			}
		} else {
			break
		}
	}
}

var passThrough = func(_ context.Context, i []interface{}) ([]interface{}, error) {
	batchCount.Inc()
	setBatchSizeIfBigger(int32(len(i)))
	return i, nil
}

var passSlow = func(_ context.Context, i []interface{}) ([]interface{}, error) {
	batchCount.Inc()
	setBatchSizeIfBigger(int32(len(i)))
	time.Sleep(2 * time.Millisecond)
	return i, nil
}

var passRandom = func(_ context.Context, i []interface{}) ([]interface{}, error) {
	batchCount.Inc()
	setBatchSizeIfBigger(int32(len(i)))
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	return i, nil
}

var sequential100 = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}

func Test_Order(t *testing.T) {

	testCases := []struct {
		name                            string
		input                           []int
		batchSize                       int
		maxConcurrency                  int
		fnc                             HammerFunc
		expectedBatchCountBetween       []int32
		expectedBiggestBatchSizeBetween []int32
		expectedTimeout                 bool
		timeoutValue                    time.Duration
	}{
		{
			name:                            "is_in_batch",
			input:                           sequential100,
			fnc:                             passThrough,
			batchSize:                       5,
			maxConcurrency:                  1,
			expectedBatchCountBetween:       []int32{20, 60},
			expectedBiggestBatchSizeBetween: []int32{2, 5},
			timeoutValue:                    time.Millisecond * 10,
		},
		{
			name:                            "is_not_in_batch",
			input:                           sequential100,
			fnc:                             passThrough,
			batchSize:                       1,
			maxConcurrency:                  5,
			expectedBatchCountBetween:       []int32{100, 100},
			expectedBiggestBatchSizeBetween: []int32{1, 1},
			timeoutValue:                    time.Millisecond * 10,
		},

		{
			name:                            "is_concurrent",
			input:                           sequential100,
			fnc:                             passSlow, // 2ms*100 > 10ms timeout
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
			fnc:             passSlow, // 2ms * 10 > 10ms
			batchSize:       1,
			maxConcurrency:  1,
			expectedTimeout: true,
			timeoutValue:    time.Millisecond * 10,
		},
		{
			name:           "stays_ordered_with_random_processing_time",
			input:          append(sequential100, sequential100...),
			fnc:            passRandom,
			batchSize:      3,
			maxConcurrency: 20,
			timeoutValue:   time.Second,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			defer func() { batchCount.Store(0); biggestBatchSize.Store(0) }()
			h := NewHammer(test.batchSize, test.maxConcurrency, test.fnc)
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