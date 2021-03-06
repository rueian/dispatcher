package pkg

import (
	"sync/atomic"
)

type writeType int

const (
	writeAck writeType = iota
	writeSent
)

type write struct {
	writeType writeType
	message   Message
}

type Persistence interface {
	ReadNext() (Message, error)
	WriteAck(Message)
	WriteSent(Message)
	Close() error
	Size() int
}

type LogPersistence struct {
	checkpoint uint64
	confirmed  Heap
	writes     chan write

	mid   uint64
	_     [8]uint64
	acks  uint64
	_     [7]uint64
	sents uint64
	_     [7]uint64
}

func (l *LogPersistence) ReadNext() (Message, error) {
	l.mid++
	return &message{id: l.mid}, nil
}

func (l *LogPersistence) WriteAck(message Message) {
	// TODO: find out when to close l.writes
	l.writes <- write{writeType: writeAck, message: message}
}

func (l *LogPersistence) WriteSent(message Message) {
	// TODO: find out when to close l.writes
	l.writes <- write{writeType: writeSent, message: message}
}

func (l *LogPersistence) start() {
	for w := range l.writes {
		switch w.writeType {
		case writeAck:
			if w.message.ID() == l.checkpoint+1 {
				l.checkpoint++
				atomic.AddUint64(&l.acks, 1)
				continue
			}

			l.confirmed.Push(w.message.ID())
			for {
				minAck, ok := l.confirmed.Peek()
				if ok && l.checkpoint+1 == minAck {
					l.checkpoint++
					l.confirmed.Pop()
				} else {
					break
				}
			}
			atomic.AddUint64(&l.acks, 1)
			// TODO write ack
		case writeSent:
			atomic.AddUint64(&l.sents, 1)
			// TODO write sent
		}
	}
}

func (l *LogPersistence) Close() error {
	return nil
}

func (l *LogPersistence) Stats() uint64 {
	return atomic.LoadUint64(&l.acks)
}

func (l *LogPersistence) Size() int {
	return l.confirmed.Cap()
}

func NewLogPersistence(size int) *LogPersistence {
	p := &LogPersistence{
		confirmed: newHeap(size),
		writes:    make(chan write, size),
	}
	go p.start()
	return p
}
