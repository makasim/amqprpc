package amqprpc

import (
	"context"
	"errors"
	"sync"

	"log"

	"time"

	"github.com/google/uuid"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/middleware"
	"github.com/streadway/amqp"
)

var Canceled = errors.New("amqprpc: call canceled")
var ErrShutdown = errors.New("amqprpc: client is shut down")
var ErrNotRunning = errors.New("amqprpc: client is not run")

type Client struct {
	ReplyQueue        string
	DeclareReplyQueue bool
	PreFetchCount     int
	WorkerCount       int
	ShutdownPeriod    time.Duration

	pool          *pool
	consumerConn  *amqpextra.Connection
	publisherConn *amqpextra.Connection
	closeCh       chan struct{}
	publisher     *amqpextra.Publisher
	initErr       error

	mutex    sync.Mutex
	running  bool
	closing  bool
	shutdown bool
}

func New(consumerConn *amqpextra.Connection, publisherConn *amqpextra.Connection) *Client {
	return &Client{
		PreFetchCount:     10,
		WorkerCount:       10,
		DeclareReplyQueue: true,
		ShutdownPeriod:    20 * time.Second,

		consumerConn:  consumerConn,
		publisherConn: publisherConn,
		publisher:     publisherConn.Publisher(),
		pool:          newPool(),

		closeCh: make(chan struct{}),
	}
}

func (client *Client) Run() {
	client.mutex.Lock()
	if client.running {
		client.mutex.Unlock()
		return
	}

	publisherClosedCh := make(chan struct{})
	consumerClosedCh := make(chan struct{})

	if client.ReplyQueue == "" {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()

		q, err := amqpextra.TempQueue(ctx, client.consumerConn)
		if err != nil {
			client.running = true
			client.mutex.Unlock()
			client.initErr = err

			return
		}

		client.DeclareReplyQueue = false
		client.ReplyQueue = q.Name
	}

	consumer := client.consumerConn.Consumer(client.ReplyQueue, amqpextra.WorkerFunc(client.processReply))
	consumer.SetWorkerNum(client.WorkerCount)
	consumer.SetInitFunc(client.initConsumer)
	consumer.Use(
		middleware.Recover(),
		middleware.AckNack(),
	)

	go func() {
		defer close(consumerClosedCh)

		consumer.Run()
	}()

	go func() {
		defer close(publisherClosedCh)

		client.publisher.Run()
	}()

	client.running = true
	client.mutex.Unlock()

	<-client.closeCh

	client.publisher.Close()
	<-publisherClosedCh

	if client.pool.count() > 0 {
		ticker := time.NewTicker(time.Millisecond * 200)
		defer ticker.Stop()

		timer := time.NewTimer(client.ShutdownPeriod)
		defer timer.Stop()

		for {
			select {
			case <-ticker.C:
			case <-timer.C:
				break
			}

			if client.pool.count() == 0 {
				break
			}
		}
	}

	consumer.Close()
	<-consumerClosedCh

	client.mutex.Lock()
	client.shutdown = true
	client.mutex.Unlock()
}

func (client *Client) Go(msg amqpextra.Publishing, done chan *Call) *Call {
	call := new(Call)
	call.publishing = msg

	if done == nil {
		done = make(chan *Call, 1)
	} else {
		if cap(done) == 0 {
			log.Panic("amqprpc: done channel is unbuffered")
		}
	}
	call.doneCh = done

	client.send(call)

	return call
}

func (client *Client) Call(msg amqpextra.Publishing) error {
	call := <-client.Go(msg, make(chan *Call, 1)).Done()
	return call.Error()
}

func (client *Client) Close() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.closing {
		return ErrShutdown
	}

	client.closing = true
	close(client.closeCh)

	return nil
}

func (client *Client) send(call *Call) {
	client.mutex.Lock()
	if client.shutdown || client.closing {
		client.mutex.Unlock()
		call.errored(ErrShutdown)
		return
	}

	if !client.running {
		client.mutex.Unlock()
		call.errored(ErrNotRunning)
		return
	}

	client.mutex.Unlock()

	if client.initErr != nil {
		call.errored(client.initErr)

		return
	}

	call.publishing.Message.CorrelationId = uuid.New().String()
	call.publishing.Message.ReplyTo = client.ReplyQueue
	call.publishing.ResultCh = make(chan error, 1)
	client.pool.set(call)

	client.publisher.Publish(call.publishing)

	if err := <-call.publishing.ResultCh; err != nil {
		client.pool.delete(call.publishing.Message.CorrelationId)
		call.errored(err)
	}
}

func (client *Client) processReply(_ context.Context, msg amqp.Delivery) interface{} {
	if msg.CorrelationId == "" {
		return middleware.Nack
	}

	call, ok := client.pool.fetch(msg.CorrelationId)
	if !ok {
		return middleware.Nack
	}

	call.done(msg)

	return nil
}

func (client *Client) initConsumer(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	err = ch.Qos(client.PreFetchCount, 0, false)
	if err != nil {
		return nil, nil, err
	}

	if client.DeclareReplyQueue {
		_, err := ch.QueueDeclare(
			client.ReplyQueue,
			false,
			true,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, nil, err
		}
	}

	msgCh, err := ch.Consume(
		client.ReplyQueue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	return ch, msgCh, nil
}
