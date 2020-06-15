package amqprpc

import (
	"sync"

	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

type Call struct {
	AutoAck bool

	mutex sync.Mutex

	publishing amqpextra.Publishing
	delivery   amqp.Delivery
	error      error

	doneCh chan *Call
	done   bool

	pool *pool
}

func newCall(msg amqpextra.Publishing, doneCh chan *Call, autoAck bool) *Call {
	if doneCh == nil {
		doneCh = make(chan *Call, 1)
	} else {
		if cap(doneCh) == 0 {
			log.Panic("amqprpc: ok channel is unbuffered")
		}
	}

	return &Call{
		AutoAck:    autoAck,
		publishing: msg,
		doneCh:     doneCh,
	}
}

func (call *Call) Publishing() amqpextra.Publishing {
	return call.publishing
}

func (call *Call) Delivery() (amqp.Delivery, error) {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	if !call.done {
		return amqp.Delivery{}, ErrNotDone
	}

	return call.delivery, call.error
}

func (call *Call) Done() <-chan *Call {
	return call.doneCh
}

func (call *Call) Cancel() {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	if call.done {
		return
	}

	call.pool.delete(call.Publishing().Message.CorrelationId)
	call.done = true
	call.error = Canceled

	// drain ok channel
	for {
		select {
		case <-call.Done():
		default:
			return
		}
	}
}

func (call *Call) errored(err error) {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	if call.done {
		return
	}

	call.pool.delete(call.Publishing().Message.CorrelationId)
	call.done = true
	call.error = err
	call.doneCh <- call
}

func (call *Call) ok(msg amqp.Delivery) bool {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	if call.done {
		return false
	}

	call.pool.delete(call.Publishing().Message.CorrelationId)
	call.done = true
	call.error = nil
	call.delivery = msg
	call.doneCh <- call

	return true
}
