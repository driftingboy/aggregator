# aggregator
轻量、简单、高效的批处理包；
可以通过设置批次大小、时间限制去提升系统吞吐量；
代码都已通过 race测试，基准测试；

## quick start

```go
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
```
go run main.go will output:
```
rec : [0 1 2 3 4 5 6 7 8 9]
rec : [10 11 12 13 14 15 16 17 18 19]
rec : [20 21 22 23 24 25 26 27 28 29]
rec : [30 31 32 33 34 35 36 37 38 39]
rec : [40 41 42 43 44 45 46 47 48 49]
rec : [50 51 52 53 54 55 56 57 58 59]
rec : [60 61 62 63 64 65 66 67 68 69]
rec : [70 71 72 73 74 75 76 77 78 79]
rec : [80 81 82 83 84 85 86 87 88 89]
rec : [90 91 92 93 94 95 96 97 98 99]
rec : [100 101 102 103 104]
```


## 详情
具体使用可参考 `aggregator_test.go`或 `example`文件夹