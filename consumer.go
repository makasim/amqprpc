package rpcclient

import (
	"context"

	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/middleware"
)

type consumer struct {
	queue         string
	declareQueue  bool
	preFetchCount int
	workerCount   int
	pool          *pool
	amqpConn      *amqpextra.Connection
}

func newConsumer(
	queue string,
	declareQueue bool,
	preFetchCount int,
	workerCount int,
	pool *pool,
	amqpConn *amqpextra.Connection,
) *consumer {
	return &consumer{
		queue:         queue,
		declareQueue:  declareQueue,
		preFetchCount: preFetchCount,
		workerCount:   workerCount,
		pool:          pool,
		amqpConn:      amqpConn,
	}
}

func (c *consumer) Run(ctx context.Context) {
	consumer := c.amqpConn.Consumer(c.queue, amqpextra.WorkerFunc(c.process))
	consumer.SetWorkerNum(c.preFetchCount)
	consumer.SetInitFunc(c.init)
	consumer.SetContext(ctx)

	consumer.Use(
		middleware.Recover(),
		middleware.AckNack(),
	)

	consumer.Run()
}

func (c *consumer) process(ctx context.Context, msg amqp.Delivery) interface{} {
	if msg.CorrelationId == "" {
		return middleware.Nack
	}

	r, ok := c.pool.get(msg.CorrelationId)
	if !ok {
		return middleware.Nack
	}

	select {
	case <-ctx.Done():
		return middleware.Requeue
	case <-r.Ctx.Done():
		return middleware.Nack
	case r.ReplyCh <- Reply{Msg: msg}:
		return middleware.Ack
	}
}

func (c *consumer) init(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	err = ch.Qos(c.preFetchCount, 0, false)
	if err != nil {
		return nil, nil, err
	}

	if c.declareQueue {
		_, err := ch.QueueDeclare(
			c.queue,
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
		c.queue,
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
