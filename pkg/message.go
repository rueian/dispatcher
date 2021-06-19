package pkg

type Message interface {
	ID() uint64
	Data() []byte
}

type message struct {
	id   uint64
	data []byte
}

func (m *message) ID() uint64 {
	return m.id
}

func (m *message) Data() []byte {
	return m.data
}
