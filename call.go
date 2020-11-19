package amqprpc

import (
	"errors"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
	"sync"
)

var ErrClosed = errors.New("amqprpc: call closed")

type Call struct {
	AutoAck bool

	message publisher.Message
	delivery   amqp.Delivery
	error      error

	mux     sync.Mutex
	closeCh chan struct{}
	doneCh  chan *Call
	done    bool

	pool *pool
}

func newCall(msg publisher.Message, doneCh chan *Call, pool *pool, autoAck bool) *Call {
	if doneCh == nil {
		doneCh = make(chan *Call, 1)
	} else if cap(doneCh) == 0 {
		panic("amqprpc: ok channel is unbuffered")
	}

	return &Call{
		AutoAck:    autoAck,
		message: msg,
		closeCh:    make(chan struct{}),
		doneCh:     doneCh,
		pool:       pool,
	}
}

func (call *Call) Message() publisher.Message {
	call.mux.Lock()
	defer call.mux.Unlock()

	return call.message
}

func (call *Call) Delivery() (amqp.Delivery, error) {
	call.mux.Lock()
	defer call.mux.Unlock()
	if !call.done {
		return amqp.Delivery{}, ErrNotDone
	}

	return call.delivery, call.error
}

func (call *Call) Done() <-chan *Call {
	return call.doneCh
}

func (call *Call) Closed() <-chan struct{} {
	return call.closeCh
}

func (call *Call) Close() {
	call.mux.Lock()
	if call.done {
		call.mux.Unlock()
		return
	}

	corrID := call.message.Publishing.CorrelationId

	call.done = true
	call.error = ErrClosed
	call.delivery = amqp.Delivery{}
	call.doneCh <- call
	close(call.closeCh)
	call.mux.Unlock()

	call.pool.delete(corrID)
}

func (call *Call) errored(err error) {
	call.mux.Lock()
	if call.done {
		call.mux.Unlock()
		return
	}

	corrID := call.message.Publishing.CorrelationId

	call.done = true
	call.error = err
	call.delivery = amqp.Delivery{}
	call.doneCh <- call
	close(call.closeCh)
	call.mux.Unlock()

	call.pool.delete(corrID)
}

func (call *Call) ok(msg amqp.Delivery) bool {
	call.mux.Lock()
	if call.done {
		call.mux.Unlock()
		return false
	}

	corrID := call.message.Publishing.CorrelationId

	call.done = true
	call.error = nil
	call.delivery = msg
	call.doneCh <- call
	close(call.closeCh)
	call.mux.Unlock()

	call.pool.delete(corrID)

	return true
}
