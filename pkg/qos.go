package pkg

import (
	"sync/atomic"
	"time"
)

type qos struct {
	rate  uint64 // per second
	quota uint64
}

func (q *qos) start(fn func()) {
	for {
		atomic.StoreUint64(&q.quota, atomic.LoadUint64(&q.rate))
		fn()
		time.Sleep(time.Second)
	}
}

func (q *qos) Consumable() bool {
	if atomic.LoadUint64(&q.rate) == 0 {
		return true
	}
	for {
		tokens := atomic.LoadUint64(&q.quota)
		if tokens == 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&q.quota, tokens, tokens-1) {
			return true
		}
	}
}

type QosSource struct {
	qos   *qos
	ring  *ring
	store Persistence
	fetch chan int
}

func (q *QosSource) Next() (Message, bool) {
	if q.qos.Consumable() {
		return q.ring.Next()
	}
	return nil, false
}

func (q *QosSource) OnAck(message Message) {
	q.fetch <- 1
	q.store.WriteAck(message)
}

func (q *QosSource) OnNack(message Message) {
	q.ring.Put(message)
}

func (q *QosSource) OnSent(message Message) {
	q.store.WriteSent(message)
}

func (q *QosSource) Start(fn func()) {
	go q.qos.start(fn)
	go func() {
		for n := range q.fetch {
			for i := 0; i < n; i++ {
				if msg, err := q.store.ReadNext(); err == nil {
					q.ring.Put(msg)
				}
			}
		}
	}()
	q.fetch <- int(q.ring.size)
}

func NewQosSource(size, rate uint64, store Persistence) *QosSource {
	return &QosSource{
		qos: &qos{
			rate: rate,
		},
		ring:  newRing(size),
		store: store,
		fetch: make(chan int, size),
	}
}
