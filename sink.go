package main

import (
	"fmt"
	"log"
)

type Sink interface {
	Write(prepare int64, commit int64)
}

type StdoutSink struct {
	prev int64
}

func MakeStdoutSink() *StdoutSink {
	return &StdoutSink{
		prev: 0,
	}
}

func (sink *StdoutSink) Write(prepare int64, commit int64) {
	fmt.Printf("prepare = \"%d\", commit = \"%d\"\n", prepare, commit)
	if sink.prev > commit {
		log.Fatal("Sink check failed")
	}
	sink.prev = commit
}

