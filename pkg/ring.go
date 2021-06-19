package pkg

import (
	"sync/atomic"
)

type ring struct {
	read  uint64
	write uint64
	size  uint64
	store []Message
}

func (r *ring) Put(m Message) {
	r.store[atomic.AddUint64(&r.write, 1)%r.size] = m
}

func (r *ring) Next() (Message, bool) {
	for {
		read := atomic.LoadUint64(&r.read)
		next := read + 1
		if next > atomic.LoadUint64(&r.write) {
			return nil, false
		}
		if atomic.CompareAndSwapUint64(&r.read, read, next) {
			return r.store[next%r.size], true
		}
	}
}

func newRing(size uint64) *ring {
	return &ring{size: size, store: make([]Message, size)}
}
