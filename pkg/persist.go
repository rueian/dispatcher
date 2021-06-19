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
}

type LogPersistence struct {
	checkpoint uint64
	confirmed  Heap
	writes     chan write

	mid   uint64
	acks  uint64
	sents uint64
}

func (l *LogPersistence) ReadNext() (Message, error) {
	l.mid++
	return &message{id: l.mid}, nil
}

func (l *LogPersistence) WriteAck(message Message) {
	l.writes <- write{writeType: writeAck, message: message}
}

func (l *LogPersistence) WriteSent(message Message) {
	l.writes <- write{writeType: writeSent, message: message}
}

func (l *LogPersistence) start() {
	for w := range l.writes {
		switch w.writeType {
		case writeAck:
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

func NewLogPersistence(size int) *LogPersistence {
	p := &LogPersistence{
		confirmed: Heap{min: make([]uint64, 0, size)},
		writes:    make(chan write, size),
	}
	go p.start()
	return p
}