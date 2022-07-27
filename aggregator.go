package aggregator

import (
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/driftingboy/aggregator/logger"
)

var DefaultLogger = logger.WithPrefix(logger.NewStdLogger(os.Stdout, 4096), logger.DefaultCaller, logger.DefaultTimer)

// Represents the aggregator
type Aggregator struct {
	option         AggregatorOption
	wg             *sync.WaitGroup
	quit           chan struct{}
	sendQ          chan interface{}
	batchProcessor BatchProcessFunc
}

// Represents the aggregator option
type AggregatorOption struct {
	// when batch size is reached, the batch will be processed
	BatchSize         int
	Workers           int
	ChannelBufferSize int
	// when DelayTime coming, the batch will be processed
	DelayTime    time.Duration
	ErrorHandler ErrorHandlerFunc
	logger       logger.Logger
	debug        bool
}

// the func to batch process items
type BatchProcessFunc func([]interface{}) error

// the func to set option for aggregator
type SetAggregatorOptionFunc func(option AggregatorOption) AggregatorOption

// the func to handle error
type ErrorHandlerFunc func(err error, items []interface{}, batchProcessFunc BatchProcessFunc, aggregator *Aggregator)

func WithBatchSize(size int) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.BatchSize = size
		return option
	}
}

func WithChannelBufferSize(size int) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.ChannelBufferSize = size
		return option
	}
}

func WithWorkerNum(workerNum int) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.Workers = workerNum
		return option
	}
}

func WithDelayTime(timeout time.Duration) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.DelayTime = timeout
		return option
	}
}

func WithErrorHandle(eh ErrorHandlerFunc) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.ErrorHandler = eh
		return option
	}
}

func WithLogger(log logger.Logger) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.logger = log
		return option
	}
}

func WithDebug(debug bool) SetAggregatorOptionFunc {
	return func(option AggregatorOption) AggregatorOption {
		option.debug = debug
		return option
	}
}

// Creates a new aggregator
func NewAggregator(batchProcessor BatchProcessFunc, optionFuncs ...SetAggregatorOptionFunc) *Aggregator {
	// default option
	option := AggregatorOption{
		BatchSize:         10,
		Workers:           runtime.NumCPU(),
		DelayTime:         1 * time.Second,
		ChannelBufferSize: 20,
		logger:            DefaultLogger,
		debug:             false,
	}

	// set option
	for _, optionFunc := range optionFuncs {
		option = optionFunc(option)
	}
	if option.ChannelBufferSize <= option.Workers {
		option.ChannelBufferSize = option.Workers
	}

	return &Aggregator{
		sendQ:          make(chan interface{}, option.ChannelBufferSize),
		option:         option,
		quit:           make(chan struct{}),
		wg:             new(sync.WaitGroup),
		batchProcessor: batchProcessor,
	}
}

// Try enqueue an item, and it is non-blocked
// note just try twice, if it fails, it will return false
func (agt *Aggregator) TryEnqueue(item interface{}) bool {
	select {
	case agt.sendQ <- item:
		return true
	default:
		if agt.option.debug {
			agt.option.logger.Log(logger.Debug, "Aggregator: Event queue is full and try reschedule")
		}

		runtime.Gosched()

		select {
		case agt.sendQ <- item:
			return true
		default:
			if agt.option.debug {
				agt.option.logger.Log(logger.Debug, "Aggregator: Event queue is still full and %+v is skipped.", item)
			}
			return false
		}
	}
}

// Enqueue an item, will be blocked if the queue is full
func (agt *Aggregator) Enqueue(item interface{}) {
	agt.sendQ <- item
}

// Start the aggregator
func (agt *Aggregator) Start() {
	for i := 0; i < agt.option.Workers; i++ {
		// wg.Add(1) must be called before the goroutine is created
		// otherwise the wg.Wait() may be not effective
		agt.wg.Add(1)
		go agt.work(i)
	}
}

// Stop the aggregator
func (agt *Aggregator) Stop() {
	close(agt.quit)
	agt.wg.Wait()
}

// Stop the aggregator safely, the difference with Stop is it guarantees no item is missed during stop
// if timeout <= 0, no timeout effective
func (agt *Aggregator) SafeStop(timeout time.Duration) {
	if len(agt.sendQ) == 0 {
		close(agt.quit)
	} else {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		if timeout > 0 {
			overTimer := time.NewTimer(timeout)
		loop:
			for {
				select {
				case <-ticker.C:
					if len(agt.sendQ) == 0 {
						close(agt.quit)
						break loop
					}
				case <-overTimer.C:
					close(agt.quit)
					break loop
				}
			}
		} else {
			for range ticker.C {
				if len(agt.sendQ) == 0 {
					close(agt.quit)
					break
				}
			}
		}
	}

	agt.wg.Wait()
}

func (agt *Aggregator) work(index int) {
	defer func() {
		if r := recover(); r != nil {
			agt.option.logger.Log(logger.Error, "Aggregator: recover worker as bad thing happens", r)
		}
	}()
	defer agt.wg.Done()

	batch := make([]interface{}, 0, agt.option.BatchSize)
	delayTimer := time.NewTimer(agt.option.DelayTime)
	defer delayTimer.Stop()

	for {
		select {
		case req := <-agt.sendQ:
			batch = append(batch, req)
			if len(batch) < agt.option.BatchSize {
				break
			}

			if agt.option.debug {
				agt.option.logger.Log(logger.Debug, "process_reason", "batch_size_reached")
			}
			agt.batchProcess(batch)

			// before reset timer, if timer is already expired,need clear delayTimer.C,
			// Otherwise the next loop may trigger the timeout logic immediately.
			if !delayTimer.Stop() {
				<-delayTimer.C
			}
			delayTimer.Reset(agt.option.DelayTime)
			batch = batch[:0]
		case <-delayTimer.C:
			if len(batch) > 0 {
				if agt.option.debug {
					agt.option.logger.Log(logger.Debug, "process_reason", "delay_timer_expired")
				}
				agt.batchProcess(batch)
				batch = batch[:0]
			}
			// must reset delayTimer, otherwise may be blocked forever(block in <-delayTimer.C)
			delayTimer.Reset(agt.option.DelayTime)
		case <-agt.quit:
			// send remind items
			if len(batch) != 0 {
				agt.batchProcess(batch)
			}

			return
		}
	}
}

func (agt *Aggregator) batchProcess(items []interface{}) {
	agt.wg.Add(1)
	defer agt.wg.Done()
	if err := agt.batchProcessor(items); err != nil {
		agt.option.logger.Log(logger.Error, "Aggregator: batchProcessor_error", err)

		if agt.option.ErrorHandler != nil {
			go agt.option.ErrorHandler(err, items, agt.batchProcessor, agt)
		}
	}
	if agt.option.debug {
		agt.option.logger.Log(logger.Debug, "Aggregator: len(items)", len(items))
	}
}

// v2 版

// refer
// https://blog.csdn.net/qq_36517296/article/details/120689095 (有问题的)
// https://github.com/travisjeffery/go-batcher/blob/cac65f09a6c939aabd0d695c9ccd8b6e9b9b5cd7/batcher.go#L29 （只支持delay的）
// https://blog.csdn.net/qq_19734597/article/details/121170593
