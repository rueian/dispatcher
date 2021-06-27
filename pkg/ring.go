package pkg

import (
	"runtime"
	"sync/atomic"
)

type ring struct {
	_     [8]uint64
	tail  uint64
	_     [7]uint64
	head  uint64
	_     [7]uint64
	mask  uint64
	_     [7]uint64
	store []Message
}

func (r *ring) Put(m Message) {
	r.store[atomic.AddUint64(&r.head, 1)&r.mask] = m
}

func (r *ring) Next() (Message, bool) {
	for {
		read := atomic.LoadUint64(&r.tail)
		next := read + 1
		if next > atomic.LoadUint64(&r.head) {
			return nil, false
		}
		if atomic.CompareAndSwapUint64(&r.tail, read, next) {
			return r.store[next&r.mask], true
		}
		runtime.Gosched()
	}
}

func (r *ring) Peak() (Message, bool) {
	read := atomic.LoadUint64(&r.tail)
	next := read + 1
	if next > atomic.LoadUint64(&r.head) {
		return nil, false
	}
	return r.store[next&r.mask], true
}

func newRing(size uint64) ring {
	size = roundUp(size)
	return ring{mask: size - 1, store: make([]Message, size)}
}

// roundUp takes a uint64 greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}
