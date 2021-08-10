package dhammer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

var testSequential100 = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}
var processedCount = atomic.NewInt32(0)

var testNailerPassThrough = func(_ context.Context, i interface{}) (interface{}, error) {
	processedCount.Inc()
	return i, nil
}

var testNailerPassSlow = func(_ context.Context, i interface{}) (interface{}, error) {
	processedCount.Inc()
	time.Sleep(10 * time.Millisecond)
	return i, nil
}

func Test_Nailer(t *testing.T) {
	testCases := []struct {
		name                   string
		inputs                 []int
		maxConcurrency         int
		fnc                    NailerFunc
		startWithPushAll       bool
		expectedTimeout        bool
		expectedProcessedCount int32
		timeoutValue           time.Duration
	}{
		{
			name:                   "is_in_batch",
			inputs:                 testSequential100,
			fnc:                    testNailerPassThrough,
			maxConcurrency:         3,
			expectedProcessedCount: int32(len(testSequential100)),
			timeoutValue:           time.Second * 10,
		},
		{
			name:            "will_timeout",
			inputs:          []int{0, 1, 2, 3, 4, 5, 6, 7, 9, 10},
			fnc:             testNailerPassSlow, // 2ms * 10 > 10ms
			maxConcurrency:  3,
			expectedTimeout: true,
			timeoutValue:    time.Millisecond * 8,
		},
		{
			name:                   "is_in_batch_with_push_all",
			inputs:                 testSequential100,
			fnc:                    testNailerPassThrough,
			startWithPushAll:       true,
			maxConcurrency:         3,
			expectedProcessedCount: int32(len(testSequential100)),
			timeoutValue:           time.Second * 10,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			defer func() { processedCount.Store(0) }()
			n := NewNailer(test.maxConcurrency, test.fnc)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if test.startWithPushAll {
				in := make([]interface{}, len(test.inputs))
				for i, input := range test.inputs {
					in[i] = input
				}
				n.PushAll(ctx, in)
			} else {
				n.Start(ctx)
				go func() {
					for _, val := range test.inputs {
						select {
						case n.In <- val:
						case <-n.Terminating():
							return
						}
					}
					n.Close()
				}()
			}

			done := make(chan []int)
			go func() {
				var out []int
				for val := range n.Out {
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
				assert.Equal(t, test.inputs, output)
			}

			assert.Equal(t, test.expectedProcessedCount, processedCount.Load())
		})
	}
}

func Test_Drain(t *testing.T) {
	in := testSequential100
	inputs := make([]interface{}, len(in))
	for i, input := range in {
		inputs[i] = input
	}
	t.Run("testing drain function", func(t *testing.T) {
		n := NewNailer(1, testNailerPassThrough)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		n.PushAll(ctx, inputs)
		n.Drain()
		assert.Equal(t, int32(100), processedCount.Load())
	})
}

func TestNailer_WaitUntilEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := NewNailer(2, testNailerPassSlow, NailerDiscardAll())
	n.Start(ctx)

	n.In <- 1
	n.In <- 2

	n.WaitUntilEmpty(ctx)
	assert.Len(t, n.In, 0, "Input is not empty")
	assert.Len(t, n.decoupler, 0, "Decoupler is not empty")

	n.In <- 1
	n.In <- 2

	n.WaitUntilEmpty(ctx)
	assert.Len(t, n.In, 0, "Input is not empty")
	assert.Len(t, n.decoupler, 0, "Decoupler is not empty")
}

func TestNailer_ForwardsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := NewNailer(2, func(_ context.Context, i interface{}) (interface{}, error) {
		if i.(int) == 2 {
			return nil, fmt.Errorf("error #%d", i.(int))
		}

		return i, nil
	})
	n.Start(ctx)

	n.Push(ctx, 1)
	n.Push(ctx, 2)
	n.Push(ctx, 3)

	n.Close()
	n.Drain()

	// The way it is, any of the pushed element could report an error
	assert.NotNil(t, n.Err(), "Error should be set to something")
	assert.Regexp(t, "^error #[0-9]+", n.Err().Error())
}

func TestNailer_WithDiscardAll_DrainEvenOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := NewNailer(8, func(_ context.Context, i interface{}) (interface{}, error) {
		return nil, fmt.Errorf("error #%d", i.(int))
	}, NailerDiscardAll())
	n.Start(ctx)

	for i := 0; i < 32; i++ {
		n.Push(ctx, i)
	}

	n.Close()
	n.Drain()

	// The way it is, any of the pushed element could report an error
	assert.NotNil(t, n.Err(), "Error should be set to something")
	assert.Regexp(t, "^error #[0-9]+", n.Err().Error())
}
