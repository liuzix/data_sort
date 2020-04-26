package main

/*
	An implementer of a Sorter encapsulates the basic logic of sorting
	the data.
*/
type Sorter interface {
	Init([]chan data) Sorter
	SetSink(sink Sink) Sorter
	Launch()
}

