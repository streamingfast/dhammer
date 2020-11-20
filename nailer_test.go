package dhammer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dfuse-io/logging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var testSequential100 = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}
var processedCount = atomic.NewInt32(0)

func init() {
	logging.TestingOverride()
}

func setupLogger() *zap.Logger {
	logger := zap.NewNop()
	if os.Getenv("DEBUG") == "true" {
		logger, _ = zap.NewDevelopment()
		logging.Override(logger)
	}
	return logger
}

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
			name:                   "is_in_batch",
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
			n := NewNailer(test.maxConcurrency, test.fnc, setupLogger())
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
		n := NewNailer(1, testNailerPassThrough, setupLogger())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		n.PushAll(ctx, inputs)
		n.Drain()
		assert.Equal(t, int32(100), processedCount.Load())
	})

}
