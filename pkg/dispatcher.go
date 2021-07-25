package pkg

import (
	"sync"
	"sync/atomic"
)

type Source interface {
	Next() (Message, bool)
	OnAck(Message)
	OnNack(Message)
	OnSent(Message)
	Start(func())
	Size() int
}

type Dispatcher struct {
	_      [8]uint64
	nextID uint64
	_      [7]uint64

	consumers sync.Map
	quota     int64

	source Source
}

func (b *Dispatcher) dispatch(consumer *Consumer) (more bool) {
	for i := b.quota - consumer.Pending(); i > 0; i-- {
		msg, ok := b.source.Next()
		if !ok {
			return false
		}
		if err := consumer.push(msg); err != nil {
			if err == ErrConsumerUnregistered {
				b.source.OnNack(msg)
			} else {
				consumer.Unregister()
			}
			return true
		}
	}
	return true
}

func (b *Dispatcher) Register(cb ReceiveHandler) (consumer *Consumer) {
	id := atomic.AddUint64(&b.nextID, 1)
	consumer = newConsumer(id, b, cb, int(b.quota))
	b.consumers.Store(id, consumer)
	b.dispatch(consumer)
	return consumer
}

func (b *Dispatcher) unregister(consumer *Consumer) {
	b.consumers.Delete(consumer.id)
}

func (b *Dispatcher) ack(consumer *Consumer, requeue bool, msg Message) {
	if requeue {
		b.source.OnNack(msg)
	} else {
		b.source.OnAck(msg)
	}
	b.dispatch(consumer)
}

func NewDispatcher(quota int64, source Source) *Dispatcher {
	b := &Dispatcher{
		quota:  quota,
		source: source,
	}
	b.source.Start(func() {
		b.consumers.Range(func(_, consumer interface{}) bool {
			return b.dispatch(consumer.(*Consumer))
		})
	})
	return b
}
