package pkg

import (
	"sync/atomic"
)

type Source interface {
	Next() (Message, bool)
	OnAck(Message)
	OnNack(Message)
	OnSent(Message)
	Start(func())
}

type eventType int

const (
	register eventType = iota
	unregister
	dispatch
	ack
)

type event struct {
	typ      eventType
	consumer *Consumer
}

type Dispatcher struct {
	_      [8]uint64
	nextID uint64
	_      [7]uint64

	consumers map[uint64]*Consumer
	quota     int64

	source Source
	events chan event
}

func (b *Dispatcher) Dispatch() {
	for e := range b.events {
		switch e.typ {
		case register:
			b.consumers[e.consumer.id] = e.consumer
			b.dispatch(e.consumer)
		case unregister:
			delete(b.consumers, e.consumer.id)
		case ack:
			b.dispatch(e.consumer)
		case dispatch:
			for _, consumer := range b.consumers {
				if more := b.dispatch(consumer); !more {
					break
				}
			}
		}
	}
}

func (b *Dispatcher) dispatch(client *Consumer) (more bool) {
	for i := b.quota - client.Pending(); i > 0; i-- {
		msg, ok := b.source.Next()
		if !ok {
			return false
		}
		if err := client.push(msg); err != nil {
			delete(b.consumers, client.id)
			client.Unregister()
			return true
		}
	}
	return true
}

func (b *Dispatcher) Register(cb ReceiveHandler) (consumer *Consumer) {
	id := atomic.AddUint64(&b.nextID, 1)
	consumer = newConsumer(id, b, cb, int(b.quota))
	b.events <- event{typ: register, consumer: consumer}
	return consumer
}

func (b *Dispatcher) unregister(consumer *Consumer) {
	b.events <- event{typ: unregister, consumer: consumer}
}

func (b *Dispatcher) ack(consumer *Consumer, requeue bool, msg Message) {
	if requeue {
		b.source.OnNack(msg)
		return
	}
	b.source.OnAck(msg)
	b.events <- event{typ: ack, consumer: consumer}
}

func NewDispatcher(size, quota int64, source Source) *Dispatcher {
	b := &Dispatcher{
		consumers: make(map[uint64]*Consumer),
		quota:     quota,
		source:    source,
		events:    make(chan event, size),
	}
	b.source.Start(func() {
		b.events <- event{typ: dispatch}
	})
	return b
}
