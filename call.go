package amqprpc

import (
	"sync"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

type Call struct {
	mutex sync.Mutex
	error error

	publishing amqpextra.Publishing
	delivery   amqp.Delivery
	doneCh     chan *Call

	pool *pool
}

func (call *Call) Error() error {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	return call.error
}

func (call *Call) Publishing() amqpextra.Publishing {
	return call.publishing
}

func (call *Call) Delivery() amqp.Delivery {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	return call.delivery
}

func (call *Call) Done() <-chan *Call {
	return call.doneCh
}

func (call *Call) Cancel() {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	call.pool.delete(call.Publishing().Message.CorrelationId)
	call.error = Canceled

	// drain done channel
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

	call.error = err
	call.delivery = amqp.Delivery{}
	call.doneCh <- call
}

func (call *Call) done(msg amqp.Delivery) {
	call.mutex.Lock()
	defer call.mutex.Unlock()

	call.error = nil
	call.delivery = msg
	call.doneCh <- call
}
