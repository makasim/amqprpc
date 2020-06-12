package rpcclient

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/makasim/amqpextra"
	"github.com/patrickmn/go-cache"
	"github.com/streadway/amqp"
)

type Reply struct {
	Err error
	Msg amqp.Delivery
}

type request struct {
	CorrID  string
	Ctx     context.Context
	ReplyCh chan Reply
}

type RPCClient struct {
	ReplyQueue    string
	PreFetchCount int
	WorkerCount   int

	pool          *pool
	consumerConn  *amqpextra.Connection
	publisherConn *amqpextra.Connection
	publisher     *amqpextra.Publisher
}

func New(consumerConn *amqpextra.Connection, publisherConn *amqpextra.Connection) *RPCClient {
	return &RPCClient{
		PreFetchCount: 10,
		WorkerCount:   10,

		consumerConn:  consumerConn,
		publisherConn: publisherConn,
		publisher:     publisherConn.Publisher(),
		pool: &pool{
			c: cache.New(time.Minute, 5*time.Minute),
		},
	}
}

func (c *RPCClient) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	declareQueue := true
	if c.ReplyQueue == "" {
		q, err := amqpextra.TempQueue(ctx, c.consumerConn)
		if err != nil {
			return err
		}

		declareQueue = false
		c.ReplyQueue = q.Name
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		newConsumer(
			c.ReplyQueue,
			declareQueue,
			c.PreFetchCount,
			c.WorkerCount,
			c.pool,
			c.consumerConn,
		).Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		c.publisher.Run()
	}()

	wg.Done()

	return nil
}

func (c *RPCClient) Call(ctx context.Context, msg amqp.Publishing) <-chan Reply {
	msg.CorrelationId = uuid.New().String()
	msg.ReplyTo = c.ReplyQueue
	publishResultCh := make(chan error)
	replyCh := make(chan Reply, 1)

	c.publisher.Publish(amqpextra.Publishing{
		Exchange:  "",
		Key:       c.ReplyQueue,
		Mandatory: false,
		Immediate: false,
		WaitReady: true,
		Message:   msg,
		ResultCh:  publishResultCh,
	})

	c.pool.set(request{
		CorrID:  msg.CorrelationId,
		Ctx:     ctx,
		ReplyCh: replyCh,
	})

	select {
	case <-ctx.Done():
		replyCh <- Reply{
			Err: ctx.Err(),
			Msg: amqp.Delivery{},
		}
	case err := <-publishResultCh:
		if err != nil {
			replyCh <- Reply{
				Err: err,
				Msg: amqp.Delivery{},
			}
		}
	}

	return replyCh
}
