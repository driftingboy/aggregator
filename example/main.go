package main

import (
	"fmt"
	"time"

	ag "github.com/driftingboy/aggregator"
)

func main() {
	handlerFunc := func(batch []interface{}) error {
		fmt.Printf("rec : %v\n", batch)
		return nil
	}
	aggr := ag.NewAggregator(
		handlerFunc, ag.WithBatchSize(10), ag.WithChannelBufferSize(50),
		ag.WithWorkerNum(1), ag.WithDelayTime(50*time.Millisecond), //ag.WithDebug(true),
	)
	aggr.Start()

	for i := 0; i < 105; i++ {
		aggr.Enqueue(i)
	}

	aggr.SafeStop(-1)
}
