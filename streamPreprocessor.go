package main

import (
	"github.com/Workiva/go-datastructures/queue"
	"log"
	"math"
)

type StreamPreprocessor struct {
	m map[int64]*data
	outQ *queue.PriorityQueue
	inChannel chan data
	OutChannel chan data
}

func (s *StreamPreprocessor) run() {
	for {
		msg := <-s.inChannel

		if msg.kind == "prepare" {
			s.m[msg.prepare] = &msg
		}

		if msg.kind == "commit" {
			stored := s.m[msg.prepare]
			stored.kind = "commit"
			stored.commit = msg.commit
			_ = s.outQ.Put(stored)
			delete(s.m, stored.prepare)

			minPrepare := int64(math.MaxInt64)
			for k := range s.m {
				if k < minPrepare {
					minPrepare = k
				}
			}

			for {
				d := s.outQ.Peek().(*data)
				if d.commit < stored.commit && d.commit < minPrepare {
					_, err := s.outQ.Get(1)
					if err != nil {
						log.Fatal(err.Error())
					}

					s.OutChannel <- *d
				} else {
					break
				}
			}
		}
	}
}

func MakePreprocessedStream(c chan data) chan data {
	s := StreamPreprocessor{
		m: make(map[int64]*data),
		outQ: queue.NewPriorityQueue(10, false),
		inChannel: c,
		OutChannel: make(chan data, 10),
	}
	go s.run()
	return s.OutChannel
}