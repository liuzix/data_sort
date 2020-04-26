package main

import (
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	pq "github.com/jupp0r/go-priority-queue"
	"log"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	prepare int = iota
	commit  int = iota
)

type queueItem struct {
	prepareToken  int64
	commitToken   int64
}

func (item *queueItem) getPriority() int64 {
	return item.commitToken
}

type streamQueue struct {
	queue *queue.Queue
}

func (sq *streamQueue) peek() *queueItem {
	item, err := sq.queue.Peek()
	if err != nil {
		log.Fatal(fmt.Errorf("peek(): peeking queue: %s", err.Error()))

	}
	return item.(*queueItem)
}

func (sq *streamQueue) pop() *queueItem {
	items, err := sq.queue.Get(1)
	if err != nil {
		log.Fatal(fmt.Errorf("pop(): popping from queue: %s", err.Error()))
	}
	return items[0].(*queueItem)
}

type concurrentSorter struct {
	numStreams        int
	numThreads        int
	capacityPerStream int
	channels          []chan data
	// to avoid contention, we launch numThreads threads,
	// each writing to a priority queue
	queues            []pq.PriorityQueue
	queueLocks        []sync.Mutex
	perStreamQueue    []*streamQueue
	// number of streams currently have items that need to be dequeued
	// accessed with atomic primitives to avoid lock contention
	numStreamsPending int32
	sink              Sink
}

func MakeConcurrentSorter(numThreads int, capacityPerStream int) *concurrentSorter {
	ret := concurrentSorter{
		numThreads:        numThreads,
		capacityPerStream: capacityPerStream,
	}

	ret.queues = make([]pq.PriorityQueue, numThreads)
	for i := range ret.queues {
		ret.queues[i] = pq.New()
	}

	return &ret
}

func (sorter *concurrentSorter) Init(channels []chan data) Sorter {
	sorter.channels = make([]chan data, len(channels))
	for i := range channels {
		sorter.channels[i] = MakePreprocessedStream(channels[i])
	}
	sorter.numStreams = len(channels)

	sorter.perStreamQueue = make([]*streamQueue, len(channels))
	for i := range sorter.perStreamQueue {
		sorter.perStreamQueue[i] = &streamQueue{
			queue: queue.New(int64(sorter.capacityPerStream)),
		}
	}

	sorter.queues = make([]pq.PriorityQueue, sorter.numThreads)
	for i := range sorter.queues {
		sorter.queues[i] = pq.New()
	}

	sorter.queueLocks = make([]sync.Mutex, sorter.numThreads)
	for i := range sorter.queueLocks {
		sorter.queueLocks[i] = sync.Mutex{}
	}

	sorter.numStreamsPending = 0

	return sorter
}

func (sorter *concurrentSorter) SetSink(sink Sink) Sorter {
	sorter.sink = sink
	return sorter
}

func (sorter *concurrentSorter) runPusher(tid int, indexBegin int, indexEnd int) {
	for {
		for i := indexBegin; i < indexEnd; i++ {
			if sorter.perStreamQueue[i].queue.Len() == int64(sorter.capacityPerStream) {
				// we use sleep instead of a condition variable
				// because condition variable may become a bottleneck especially if pushers are many
				time.Sleep(time.Millisecond * 5)
				continue
			}
			select {
			case msg := <-sorter.channels[i]:
				if msg.kind == "commit" {
					item := &queueItem{
						prepareToken:  msg.prepare,
						commitToken:   msg.commit,
					}
					sorter.queueLocks[tid].Lock()
					err := sorter.perStreamQueue[i].queue.Put(item)
					if err != nil {
						log.Fatal(err.Error())
					}

					if sorter.perStreamQueue[i].queue.Len() == 1 {
						sorter.queues[tid].Insert(sorter.perStreamQueue[i], float64(msg.commit))
						atomic.AddInt32(&sorter.numStreamsPending, 1)
					}
					sorter.queueLocks[tid].Unlock()
				} else {
					log.Fatal("BUG, illegal message kind")
				}
			default:
				continue
			}
		}
	}
}

func (sorter *concurrentSorter) runPopper() {
	for {
		if atomic.LoadInt32(&sorter.numStreamsPending) < int32(sorter.numStreams) {
			runtime.Gosched()
			continue
		}

		var stream *streamQueue = nil
		var item *queueItem = nil
		var qId int
		for i := 0; i < sorter.numThreads; i++ {
			sorter.queueLocks[i].Lock()
			curStreamQueue, err := sorter.queues[i].Pop()
			if err != nil {
				log.Fatal(fmt.Errorf("getting curStreamQueue: popping from queue: %s", err.Error()))

			}

			// the library we use do not have a "Peek" method, so this is the most simple way of doing "peek"
			sorter.queues[i].Insert(curStreamQueue, float64(curStreamQueue.(*streamQueue).peek().getPriority()))

			if item == nil || curStreamQueue.(*streamQueue).peek().getPriority() < item.getPriority() {
				stream = curStreamQueue.(*streamQueue)
				item = stream.peek()
				qId = i
			}
		}

		for i := 0; i < sorter.numThreads; i++ {
			if i != qId {
				sorter.queueLocks[i].Unlock()
			}
		}

		poppedItem, err := sorter.queues[qId].Pop()
		if err != nil {
			log.Fatal(fmt.Errorf("popping from queue: %s", err.Error()))
		}

		if poppedItem != stream {
			log.Fatal("BUG, popped perStreamQueue does not match")
		}

		if item != stream.pop() {
			log.Fatal("BUG, popped item does not match")
		}

		if stream.queue.Len() > 0 {
			sorter.queues[qId].Insert(stream, float64(stream.peek().getPriority()))
		} else {
			atomic.AddInt32(&sorter.numStreamsPending, -1)
		}

		sorter.queueLocks[qId].Unlock()
		sorter.sink.Write(item.prepareToken, item.commitToken)
	}
}

func (sorter *concurrentSorter) Launch() {
	go sorter.runPopper()

	perThread := int(math.Ceil(float64(sorter.numStreams) / float64(sorter.numThreads)))
	for i := 0; i < sorter.numThreads; i++ {
		go sorter.runPusher(i, i*perThread, int(math.Min(float64(sorter.numStreams), float64((i+1)*perThread))))
	}
}
