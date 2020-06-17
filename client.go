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
var ErrPublisherUnready = errors.New("amqprpc: publisher unready")
var ErrConsumerUnready = errors.New("amqprpc: consumer unready")
var ErrShutdown = errors.New("amqprpc: client is shut down")

type options struct {
	replyQueue     ReplyQueue
	consumer       Consumer
	preFetchCount  int
	workerCount    int
	shutdownPeriod time.Duration
}

type Client struct {
	opts options

	context    context.Context
	cancelFunc context.CancelFunc

	pool            *pool
	consumerConn    *amqpextra.Connection
	consumer        *amqpextra.Consumer
	consumerCloseCh chan struct{}

	publisherConn    *amqpextra.Connection
	publisherCloseCh chan struct{}
	publisher        *amqpextra.Publisher

	setReplyQueueCh chan string
	getReplyQueueCh chan string

	closingMutex sync.Mutex
	closing      bool
}

func New(
	publisherConn *amqpextra.Connection,
	consumerConn *amqpextra.Connection,
	opts ...Option,
) (*Client, error) {
	client := &Client{
		opts: options{
			replyQueue: ReplyQueue{
				Name:       "",
				Declare:    true,
				AutoDelete: true,
				Exclusive:  true,
			},
			consumer: Consumer{
				AutoAck:   true,
				Exclusive: true,
			},
			preFetchCount:  10,
			workerCount:    10,
			shutdownPeriod: 20 * time.Second,
		},

		context: context.Background(),

		consumerConn:    consumerConn,
		consumerCloseCh: make(chan struct{}),

		publisherConn:    publisherConn,
		publisherCloseCh: make(chan struct{}),
		publisher:        publisherConn.Publisher(),

		setReplyQueueCh: make(chan string),
		getReplyQueueCh: make(chan string),

		pool: newPool(),
	}

	for _, opt := range opts {
		opt(client)
	}

	client.context, client.cancelFunc = context.WithCancel(client.context)
	client.consumer = client.consumerConn.Consumer("", amqpextra.WorkerFunc(client.reply))
	client.consumer.SetWorkerNum(client.opts.workerCount)
	client.consumer.SetInitFunc(client.initConsumer)
	client.consumer.Use(middleware.Recover(), middleware.AckNack())

	go func() {
		defer close(client.publisherCloseCh)

		client.publisher.Run()
	}()

	go func() {
		defer close(client.consumerCloseCh)

		client.consumer.Run()
	}()

	go func() {
		for {
			select {
			case replyQueue := <-client.setReplyQueueCh:
			loop:
				for {
					select {
					case client.getReplyQueueCh <- replyQueue:
					case <-client.consumer.Unready():
						break loop
					case <-client.consumerCloseCh:
						return
					}
				}
			case <-client.consumerCloseCh:
				return
			}
		}
	}()

	go func() {
		<-client.context.Done()

		client.Close()
	}()

	return client, nil
}

func (client *Client) Go(msg amqpextra.Publishing, done chan *Call) *Call {
	call := newCall(msg, done, client.pool, client.opts.consumer.AutoAck)
	client.send(call)

	return call
}

func (client *Client) Call(msg amqpextra.Publishing) (amqp.Delivery, error) {
	call := <-client.Go(msg, make(chan *Call, 1)).Done()
	return call.Delivery()
}

func (client *Client) Close() error {
	client.closingMutex.Lock()
	if client.closing {
		client.closingMutex.Unlock()
		return ErrShutdown
	}

	client.closing = true
	client.closingMutex.Unlock()

	shutdownPeriodTimer := time.NewTimer(client.opts.shutdownPeriod)
	defer shutdownPeriodTimer.Stop()

	client.publisher.Close()
	select {
	case <-client.publisherCloseCh:
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
				<-client.consumerCloseCh

				client.consumerConn.Close()
				client.publisherConn.Close()

				return fmt.Errorf("amqprpc: shutdown grace period time out: some calls have not been done")
			}

			if client.pool.count() == 0 {
				break loop
			}
		}
	}

	client.consumer.Close()
	select {
	case <-client.consumerCloseCh:
	case <-shutdownPeriodTimer.C:
		return fmt.Errorf("amqprpc: shutdown grace period time out: consumer not stopped")
	}

	client.consumerConn.Close()
	client.publisherConn.Close()

	client.cancelFunc()

	return nil
}

func (client *Client) send(call *Call) {
	publisherUnreadyCh := client.publisher.Unready()
	consumerUnreadyCh := client.consumer.Unready()
	if call.publishing.WaitReady {
		publisherUnreadyCh = nil
		consumerUnreadyCh = nil
	}

	select {
	case replyQueue := <-client.getReplyQueueCh:
		call.publishing.Message.ReplyTo = replyQueue
		call.publishing.Message.CorrelationId = uuid.New().String()
		call.publishing.ResultCh = make(chan error, 1)
		client.pool.set(call)

		client.publisher.Publish(call.publishing)

		select {
		case err := <-call.publishing.ResultCh:
			if err != nil {
				client.pool.delete(call.publishing.Message.CorrelationId)
				call.errored(err)
			}

			return
		case <-client.publisherCloseCh:
			call.errored(ErrShutdown)
			return
		}
	//noinspection GoNilness
	case <-consumerUnreadyCh:
		call.errored(ErrConsumerUnready)
		return
	//noinspection GoNilness
	case <-publisherUnreadyCh:
		call.errored(ErrPublisherUnready)
		return
	case <-call.doneCh:
		return
	case <-client.publisherCloseCh:
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

	err = ch.Qos(client.opts.preFetchCount, 0, false)
	if err != nil {
		return nil, nil, err
	}

	var replyQueue string
	if client.opts.replyQueue.Name == "" {
		rq := client.opts.replyQueue

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

		replyQueue = q.Name
	} else if client.opts.replyQueue.Declare {
		rq := client.opts.replyQueue

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

		replyQueue = q.Name
	} else {
		replyQueue = client.opts.replyQueue.Name
	}

	c := client.opts.consumer
	msgCh, err := ch.Consume(
		replyQueue,
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

	client.setReplyQueueCh <- replyQueue

	return ch, msgCh, nil
}
