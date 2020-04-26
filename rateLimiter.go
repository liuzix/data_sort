package main

import "time"

type RateLimiter struct{
	nano int64
	counter uint
	limit uint
	downstream Sink
}

func (r *RateLimiter) Write(prepare int64, commit int64) {
	if time.Now().UnixNano() / 100000000 != r.nano / 100000000 {
		r.nano = time.Now().UnixNano()
		r.counter = 0
	}
	if r.limit <= r.counter {
		time.Sleep(time.Duration(100000000 + r.nano - time.Now().UnixNano()))
	}
	r.downstream.Write(prepare, commit)
	r.counter ++
}

func MakeRateLimiter(limit uint, sink Sink) Sink {
	return &RateLimiter{
		nano: 0,
		counter: 0,
		limit: limit,
		downstream: sink,
	}
}
