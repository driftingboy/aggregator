package aggregator

import (
	"fmt"
	"testing"
	"time"
)

// TODO 增加partition

//Output:
//rec : [0 1 2 3 4 5 6 7 8 9]
//rec : [10 11 12 13 14 15 16 17 18 19]
//rec : [20 21]
//rec : [22 23 24 25 26 27 28]
func TestFlowWithDataRace(t *testing.T) {
	// make aggr
	handlerFunc := func(batch []interface{}) error {
		fmt.Printf("rec : %v\n", batch)
		return nil
	}
	aggr := NewAggregator(handlerFunc, WithWorkerNum(1), WithDelayTime(50*time.Millisecond))
	aggr.Start()

	// send data
	exit := make(chan struct{})
	go func() {
		for i := 0; i < 22; i++ {
			aggr.Enqueue(i)
		}
		time.Sleep(150 * time.Millisecond)
		for i := 22; i < 29; i++ {
			aggr.Enqueue(i)
		}
	}()
	go func() {
		time.Sleep(200 * time.Millisecond)
		aggr.SafeStop(3 * time.Second)
		exit <- struct{}{}
	}()

	<-exit
}

// cpu: Intel(R) Core(TM) i7-7567U CPU @ 3.50GHz
// BenchmarkFlow-4          6341659               167.8 ns/op             8 B/op          0 allocs/op
func BenchmarkFlow(b *testing.B) {
	handlerFunc := func(batch []interface{}) error {
		return nil
	}
	aggr := NewAggregator(handlerFunc, WithBatchSize(50), WithChannelBufferSize(500), WithWorkerNum(1), WithDelayTime(50*time.Millisecond))
	aggr.Start()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggr.Enqueue(i)
	}

	aggr.SafeStop(-1)
}
