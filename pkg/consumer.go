package pkg

import (
	"errors"
	"sync/atomic"
)

var ErrConsumerUnregistered = errors.New("consumer unregistered")

type ReceiveHandler func(msg Message) error

func newConsumer(id uint64, dispatcher *Dispatcher, onReceived ReceiveHandler, size int) *Consumer {
	return &Consumer{
		id:         id,
		dispatcher: dispatcher,
		onReceived: onReceived,
		msgs:       newRing(uint64(size)),
		acks:       newHeap(size),
	}
}

type Consumer struct {
	id         uint64
	dispatcher *Dispatcher
	onReceived ReceiveHandler

	msgs ring
	acks Heap

	_     [8]uint64
	count int64
	_     [7]uint64
	state int64
	_     [7]uint64
}

func (c *Consumer) Pending() (count int64) {
	return atomic.LoadInt64(&c.count)
}

func (c *Consumer) push(msg Message) error {
	if atomic.LoadInt64(&c.state) != 0 {
		return ErrConsumerUnregistered
	}
	atomic.AddInt64(&c.count, 1)
	c.msgs.Put(msg)
	return c.onReceived(msg)
}

func (c *Consumer) Ack(msg Message, requeue bool) {
	if atomic.LoadInt64(&c.state) != 0 {
		return
	}
	c.acks.Push(msg.ID())
	for {
		ack, _ := c.acks.Peek()
		msg, ok := c.msgs.Peak()
		if !ok {
			panic("consumer ring is broken")
		}
		if msg.ID() == ack {
			c.acks.Pop()
			c.msgs.Next()
			atomic.AddInt64(&c.count, -1)
		} else {
			break
		}
	}
	c.dispatcher.ack(c, requeue, msg)
}

func (c *Consumer) Unregister() {
	if !atomic.CompareAndSwapInt64(&c.state, 0, 1) {
		return
	}
	c.dispatcher.unregister(c)

	acks := make(map[uint64]bool, c.acks.Len())
	for {
		ack, ok := c.acks.Pop()
		if !ok {
			break
		}
		acks[ack] = true
	}
	for {
		msg, ok := c.msgs.Next()
		if !ok {
			break
		}
		if !acks[msg.ID()] {
			c.dispatcher.ack(nil, true, msg)
		}
	}
}
