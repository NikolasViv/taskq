package msgqueue

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// Handler is an interface for processing messages.
type Handler interface {
	HandleMessage(msg *Message) error
}

type HandlerFunc func(*Message) error

func (fn HandlerFunc) HandleMessage(msg *Message) error {
	return fn(msg)
}

type reflectFunc struct {
	fv reflect.Value // Kind() == reflect.Func
	ft reflect.Type

	compress bool
}

var _ Handler = (*reflectFunc)(nil)

func NewHandler(fn interface{}) Handler {
	if h, ok := fn.(Handler); ok {
		return h
	}

	h := reflectFunc{
		fv: reflect.ValueOf(fn),
	}
	h.ft = h.fv.Type()
	if h.ft.Kind() != reflect.Func {
		panic(fmt.Sprintf("got %s, wanted %s", h.ft.Kind(), reflect.Func))
	}
	return &h
}

func (h *reflectFunc) HandleMessage(msg *Message) error {
	b, err := msg.MarshalArgs()
	if err != nil {
		return err
	}

	dec := msgpack.NewDecoder(bytes.NewBuffer(b))
	in := make([]reflect.Value, h.ft.NumIn())
	for i := 0; i < h.ft.NumIn(); i++ {
		arg := reflect.New(h.ft.In(i)).Elem()
		err = dec.DecodeValue(arg)
		if err != nil {
			err = fmt.Errorf(
				"msgqueue: decoding arg=%d failed (data=%.100x): %s", i, b, err)
			break
		}
		in[i] = arg
	}

	out := h.fv.Call(in)
	if n := h.ft.NumOut(); n > 0 && h.ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			return errv.Interface().(error)
		}
	}

	return nil
}
