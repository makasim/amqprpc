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
	consumerConn     *amqpextra.Connection
	consumer         *amqpextra.Consumer
	consumerClosedCh chan struct{}

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

	if client.replyQueueOpt.Name == "" {
		if !client.replyQueueOpt.Declare {
			return nil, fmt.Errorf("amqprpc: either set queue or set declare true")
		}

		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()

		q, err := amqpextra.DeclareTempQueue(ctx, client.consumerConn)
		if err != nil {
			return nil, fmt.Errorf("amqprpc: %w", err)
		}

		client.replyQueueOpt.Declare = false
		client.replyQueueOpt.Name = q.Name
	}

	client.consumer = client.consumerConn.Consumer(client.replyQueueOpt.Name, amqpextra.WorkerFunc(client.reply))
	client.consumer.SetWorkerNum(client.workerCountOpt)
	client.consumer.SetInitFunc(client.initConsumer)
	client.consumer.Use(middleware.Recover(), middleware.AckNack())

	go func() {
		defer close(client.publisherClosedCh)

		client.publisher.Run()
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

	client.mux.Unlock()

	call.publishing.Message.CorrelationId = uuid.New().String()
	call.publishing.Message.ReplyTo = client.replyQueueOpt.Name
	call.publishing.ResultCh = make(chan error, 1)
	client.pool.set(call)

	client.publisher.Publish(call.publishing)

	if err := <-call.publishing.ResultCh; err != nil {
		client.pool.delete(call.publishing.Message.CorrelationId)
		call.errored(err)
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

	queueName := client.replyQueueOpt.Name
	if client.replyQueueOpt.Declare {
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

		queueName = q.Name
	}

	c := client.consumerOpt
	msgCh, err := ch.Consume(
		queueName,
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
