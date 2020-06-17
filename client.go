package amqprpc

import (
	"context"
	"errors"
	"sync"

	"time"

	"fmt"

	"github.com/google/uuid"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/middleware"
	"github.com/streadway/amqp"
)

var ErrNotDone = errors.New("amqprpc: call is not done")
var ErrConsumerUnready = errors.New("amqprpc: consumer unready")
var ErrPublisherUnready = errors.New("amqprpc: publisher unready")
var ErrShutdown = errors.New("amqprpc: client is shut down")

type Client struct {
	replyQueueOpt     ReplyQueue
	consumerOpt       Consumer
	preFetchCountOpt  int
	workerCountOpt    int
	shutdownPeriodOpt time.Duration

	context    context.Context
	cancelFunc context.CancelFunc

	pool             *pool
	consumerReady    bool
	consumerConn     *amqpextra.Connection
	consumer         *amqpextra.Consumer
	consumerClosedCh chan struct{}

	publisherReady    bool
	publisherConn     *amqpextra.Connection
	publisherClosedCh chan struct{}
	publisher         *amqpextra.Publisher

	mux     sync.Mutex
	closing bool
}

func New(
	publisherConn *amqpextra.Connection,
	consumerConn *amqpextra.Connection,
	opts ...Option,
) (*Client, error) {
	client := &Client{
		replyQueueOpt: ReplyQueue{
			Name:       "",
			Declare:    true,
			AutoDelete: true,
			Exclusive:  true,
		},
		consumerOpt: Consumer{
			AutoAck:   true,
			Exclusive: true,
		},
		preFetchCountOpt:  10,
		workerCountOpt:    10,
		shutdownPeriodOpt: 20 * time.Second,

		context: context.Background(),

		consumerConn:     consumerConn,
		consumerClosedCh: make(chan struct{}),

		publisherConn:     publisherConn,
		publisherClosedCh: make(chan struct{}),
		publisher:         publisherConn.Publisher(),

		pool: newPool(),
	}

	for _, opt := range opts {
		opt(client)
	}

	client.context, client.cancelFunc = context.WithCancel(client.context)

	client.consumer = client.consumerConn.Consumer("", amqpextra.WorkerFunc(client.reply))
	client.consumer.SetWorkerNum(client.workerCountOpt)
	client.consumer.SetInitFunc(client.initConsumer)
	client.consumer.Use(middleware.Recover(), middleware.AckNack())

	go func() {
		defer close(client.publisherClosedCh)

		client.publisher.Run()
	}()

	go func() {
		for {
			select {
			case <-client.consumer.Ready():
				client.mux.Lock()
				client.consumerReady = true
				client.mux.Unlock()
				select {
				case <-client.consumer.Unready():
					client.mux.Lock()
					client.consumerReady = false

					// Cancel calls if auto-delete or temp queue is used.
					// If connection lost the reply queue is deleted hence we wont get reply.
					var deadCalls map[string]*Call
					if client.replyQueueOpt.Name == "" || client.replyQueueOpt.Declare {
						deadCalls = client.pool.reset()
					}
					client.mux.Unlock()

					for _, call := range deadCalls {
						call.errored(ErrConsumerUnready)
					}
				case <-client.consumerClosedCh:
					return
				}
			case <-client.consumerClosedCh:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-client.publisher.Ready():
				client.mux.Lock()
				client.publisherReady = true
				client.mux.Unlock()
				select {
				case <-client.publisher.Unready():
					client.mux.Lock()
					client.publisherReady = false
					client.mux.Unlock()
				case <-client.publisherClosedCh:
					return
				}
			case <-client.publisherClosedCh:
				return
			}
		}
	}()

	go func() {
		defer close(client.consumerClosedCh)

		client.consumer.Run()
	}()

	go func() {
		<-client.context.Done()

		client.Close()
	}()

	return client, nil
}

func (client *Client) Go(msg amqpextra.Publishing, done chan *Call) *Call {
	call := newCall(msg, done, client.pool, client.consumerOpt.AutoAck)
	client.send(call)

	return call
}

func (client *Client) Call(msg amqpextra.Publishing) (amqp.Delivery, error) {
	call := <-client.Go(msg, make(chan *Call, 1)).Done()
	return call.Delivery()
}

func (client *Client) Close() error {
	client.mux.Lock()

	if client.closing {
		client.mux.Unlock()

		return ErrShutdown
	}

	client.closing = true
	client.mux.Unlock()

	shutdownPeriodTimer := time.NewTimer(client.shutdownPeriodOpt)
	defer shutdownPeriodTimer.Stop()

	client.publisher.Close()
	select {
	case <-client.publisherClosedCh:
	case <-shutdownPeriodTimer.C:
		return fmt.Errorf("amqprpc: shutdown grace period time out: publisher not stopped")
	}

	if client.pool.count() > 0 {
		ticker := time.NewTicker(time.Millisecond * 200)
		defer ticker.Stop()

	loop:
		for {
			select {
			case <-ticker.C:
			case <-shutdownPeriodTimer.C:
				client.cancelFunc()
				client.consumer.Close()
				<-client.consumerClosedCh

				client.consumerConn.Close()
				client.publisherConn.Close()

				return fmt.Errorf("amqprpc: shutdown grace period time out: some calls have not been done")
			}

			if client.pool.count() == 0 {
				break loop
			}
		}
	}

	client.cancelFunc()
	client.consumer.Close()
	select {
	case <-client.consumerClosedCh:
	case <-shutdownPeriodTimer.C:
		return fmt.Errorf("amqprpc: shutdown grace period time out: consumer not stopped")
	}

	client.consumerConn.Close()
	client.publisherConn.Close()

	return nil
}

func (client *Client) send(call *Call) {
	client.mux.Lock()
	if client.closing {
		client.mux.Unlock()
		call.errored(ErrShutdown)
		return
	}
	if !client.consumerReady {
		call.errored(ErrConsumerUnready)
		return
	}
	if !client.publisherReady {
		call.errored(ErrPublisherUnready)
		return
	}

	call.publishing.Message.ReplyTo = client.replyQueueOpt.name
	call.publishing.Message.CorrelationId = uuid.New().String()
	call.publishing.ResultCh = make(chan error, 1)
	client.pool.set(call)

	client.mux.Unlock()
	client.publisher.Publish(call.publishing)

	select {
	case err := <-call.publishing.ResultCh:
		if err != nil {
			client.pool.delete(call.publishing.Message.CorrelationId)
			call.errored(err)
		}

		return
	case <-client.publisherClosedCh:
		call.errored(ErrShutdown)
		return
	}
}

func (client *Client) reply(_ context.Context, msg amqp.Delivery) interface{} {
	if msg.CorrelationId == "" {
		return middleware.Nack
	}

	call, ok := client.pool.fetch(msg.CorrelationId)
	if !ok {
		return middleware.Nack
	}

	if !call.ok(msg) {
		return middleware.Nack
	}

	return nil
}

func (client *Client) initConsumer(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	err = ch.Qos(client.preFetchCountOpt, 0, false)
	if err != nil {
		return nil, nil, err
	}

	if client.replyQueueOpt.Name == "" {
		rq := client.replyQueueOpt

		q, err := ch.QueueDeclare(
			"",
			false,
			true,
			true,
			rq.NoWait,
			rq.Args,
		)
		if err != nil {
			return nil, nil, err
		}

		client.replyQueueOpt.name = q.Name
	} else if client.replyQueueOpt.Declare {
		rq := client.replyQueueOpt

		q, err := ch.QueueDeclare(
			rq.Name,
			rq.Durable,
			rq.AutoDelete,
			rq.Exclusive,
			rq.NoWait,
			rq.Args,
		)
		if err != nil {
			return nil, nil, err
		}

		client.replyQueueOpt.name = q.Name
	}

	c := client.consumerOpt
	msgCh, err := ch.Consume(
		client.replyQueueOpt.name,
		c.Tag,
		c.AutoAck,
		c.Exclusive,
		c.NoLocal,
		c.NoWait,
		c.Args,
	)
	if err != nil {
		return nil, nil, err
	}

	return ch, msgCh, nil
}
