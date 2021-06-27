package pkg

import (
	"errors"
	"sync"
	"sync/atomic"
)

type ReceiveHandler func(msg Message) error

func newConsumer(id uint64, dispatcher *Dispatcher, onReceived ReceiveHandler, size int) *Consumer {
	return &Consumer{
		id:         id,
		dispatcher: dispatcher,
		onReceived: onReceived,
		pending:    make(map[uint64]Message, size),
	}
}

type Consumer struct {
	id         uint64
	dispatcher *Dispatcher
	onReceived ReceiveHandler

	mu      sync.Mutex
	pending map[uint64]Message
	_       [8]uint64
	count   int64
	_       [7]uint64
}

func (c *Consumer) Pending() (count int64) {
	return atomic.LoadInt64(&c.count)
}

func (c *Consumer) push(msg Message) error {
	c.mu.Lock()
	if c.dispatcher == nil {
		c.mu.Unlock()
		return errors.New("consumer unregistered")
	}
	c.pending[msg.ID()] = msg
	c.mu.Unlock()
	atomic.AddInt64(&c.count, 1)
	return c.onReceived(msg)
}

func (c *Consumer) Ack(msg Message, requeue bool) {
	c.mu.Lock()
	if c.dispatcher == nil {
		c.mu.Unlock()
		return
	}
	delete(c.pending, msg.ID())
	c.mu.Unlock()
	atomic.AddInt64(&c.count, -1)
	c.dispatcher.ack(c, requeue, msg)
}

func (c *Consumer) Unregister() {
	c.mu.Lock()
	if c.dispatcher == nil {
		c.mu.Unlock()
		return
	}
	broker := c.dispatcher
	pending := c.pending
	c.pending = nil
	c.dispatcher = nil
	c.mu.Unlock()
	broker.unregister(c)
	for _, m := range pending {
		broker.ack(c, true, m)
	}
	atomic.StoreInt64(&c.count, 0)
}
