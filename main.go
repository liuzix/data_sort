package main

import (
	"flag"
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	clientNums    = 20
	messageNums   = 20000
	dataStreaming []chan data

	token int64
	step  int64

	maxSleepInterval int64 = 5
	maxGap           int64 = 10

	wg sync.WaitGroup
	sorter Sorter
	sink Sink
)

type data struct {
	kind    string
	prepare int64
	commit  int64
}

func (d *data) Compare(other queue.Item) int {
	if d.commit > other.(*data).commit {
		return 1
	}
	if d.commit == other.(*data).commit {
		return 0
	}
	return -1
}

func init() {
	dataStreaming = make([]chan data, clientNums)
	for i := 0; i < clientNums; i++ {
		dataStreaming[i] = make(chan data, messageNums)
	}
}

/* 1. 请实现 collectAndSort 函数：假设数据源产生的数据是无休无止的，请以最快的方法输出排序后的结果
 * 2. 考虑流控，控制内存的使用
 */
func main() {
	numThreads := flag.Int("n", 5, "number of pusher threads")
	capacityPerStream := flag.Int("l", 10, "queue length for each stream")
	rate := flag.Int("r", 0, "rate limit per 0.1 second, 0 means unlimited")
	flag.Parse()
	fmt.Printf("numThreads = %d, capacityPerStream = %d\n", *numThreads, *capacityPerStream)
	sorter = MakeConcurrentSorter(*numThreads, *capacityPerStream)
	if *rate > 0 {
		sink = MakeRateLimiter(uint(*rate), MakeStdoutSink())
	} else {
		sink = MakeStdoutSink()
	}

	wg.Add(clientNums*2 + 1)
	for i := 0; i < clientNums; i++ {
		go func(index int) {
			defer wg.Done()
			generateDatas(index)
		}(i)
		go func(index int) {
			defer wg.Done()
			generateDatas(index)
		}(i)
	}

	go func() {
		defer wg.Done()
		collectAndSort()
	}()
	wg.Wait()
}

func collectAndSort() {
	sorter.Init(dataStreaming).SetSink(sink).Launch()
}

func generateDatas(index int) {
	for i := 0; i < messageNums; i++ {
		prepare := incrementToken()
		sleep(maxSleepInterval)

		dataStreaming[index] <- data{
			kind:    "prepare",
			prepare: prepare,
		}
		sleep(maxSleepInterval)

		commit := incrementToken()
		sleep(maxSleepInterval)

		dataStreaming[index] <- data{
			kind:    "commit",
			prepare: prepare,
			commit:  commit,
		}
		sleep(10 * maxSleepInterval)
	}
}

func incrementToken() int64 {
	return atomic.AddInt64(&token, rand.Int63()%maxGap+1)
}

func sleep(factor int64) {
	interval := atomic.AddInt64(&step, 3)%factor + 1
	waitTime := time.Duration(rand.Int63() % interval)
	time.Sleep(waitTime * time.Millisecond)
}
