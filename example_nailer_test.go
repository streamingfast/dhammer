package dhammer_test

import (
	"context"
	"fmt"
	"strconv"

	"github.com/streamingfast/dhammer"
)

func ExampleNailer() {
	nailer := dhammer.NewNailer(2, func(ctx context.Context, i int) (string, error) {
		return strconv.FormatInt(int64(i*2), 10), nil
	})

	ctx := context.Background()
	nailer.Start(ctx)

	done := make(chan bool, 1)
	go func() {
		for out := range nailer.Out {
			fmt.Println("Received", out)
		}

		done <- true
	}()

	for _, i := range []int{1, 2, 3, 4, 5, 6, 7, 8} {
		nailer.Push(ctx, i)
	}

	nailer.Close()
	<-done

	fmt.Println("Completed")
	// Output:
	// Received 2
	// Received 4
	// Received 6
	// Received 8
	// Received 10
	// Received 12
	// Received 14
	// Received 16
	// Completed
}
