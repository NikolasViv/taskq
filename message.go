package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/valyala/gozstd"
	"github.com/vmihailenco/msgpack"

	"github.com/vmihailenco/taskq/internal"
)

// ErrDuplicate is returned when adding duplicate message to the queue.
var ErrDuplicate = errors.New("taskq: message with such name already exists")

// Message is used to create and retrieve messages from a queue.
type Message struct {
	Ctx context.Context

	// SQS/IronMQ message id.
	ID string `msgpack:",omitempty"`

	// Optional name for the message. Messages with the same name
	// are processed only once.
	Name string `msgpack:"-"`

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration `msgpack:"-"`

	// Function args passed to the handler.
	Args []interface{} `msgpack:"-"`

	// Binary representation of the args.
	ArgsCompressed bool
	ArgsBin        []byte

	// SQS/IronMQ reservation id that is used to release/delete the message.
	ReservationID string `msgpack:"-"`

	// The number of times the message has been reserved or released.
	ReservedCount int

	TaskName  string
	Task      *Task `msgpack:"-"`
	StickyErr error `msgpack:"-"`

	marshalBinaryCache []byte
}

func NewMessage(args ...interface{}) *Message {
	return &Message{
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q ReservedCount=%d>",
		m.ID, m.Name, m.ReservedCount)
}

func (m *Message) OnceWithArgs(period time.Duration, args ...interface{}) {
	m.Name = fmt.Sprintf("%s-%s-%d", hashArgs(args), period, timeSlot(period))
	m.Delay = period + 5*time.Second
}

func (m *Message) MarshalArgs() ([]byte, error) {
	if m.TaskName == "" {
		return nil, internal.ErrTaskNameRequired
	}

	if m.ArgsBin != nil {
		if !m.ArgsCompressed {
			return m.ArgsBin, nil
		}
		if m.Args == nil {
			return gozstd.Decompress(nil, m.ArgsBin)
		}
	}

	b, err := msgpack.Marshal(m.Args)
	if err != nil {
		return nil, err
	}
	m.ArgsBin = b
	return b, nil
}

func (m *Message) MarshalBinary() ([]byte, error) {
	if m.marshalBinaryCache != nil {
		return m.marshalBinaryCache, nil
	}

	_, err := m.MarshalArgs()
	if err != nil {
		return nil, err
	}

	if !m.ArgsCompressed && len(m.ArgsBin) > 512 {
		compressed := gozstd.Compress(nil, m.ArgsBin)
		if len(compressed) < len(m.ArgsBin) {
			m.ArgsCompressed = true
			m.ArgsBin = compressed
		}
	}

	b, err := msgpack.Marshal(m)
	if err != nil {
		return nil, err
	}

	m.marshalBinaryCache = b
	return b, nil
}

func (m *Message) UnmarshalBinary(b []byte) error {
	err := msgpack.Unmarshal(b, m)
	if err != nil {
		return err
	}

	if m.ArgsCompressed {
		b, err = gozstd.Decompress(nil, m.ArgsBin)
		if err != nil {
			return err
		}

		m.ArgsCompressed = false
		m.ArgsBin = b
	}

	return nil
}
