package amqprpc

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type ReplyQueue struct {
	Name       string // user provided queue name, could be empty
	name       string // user provided queue name, or if name empty: temporary queue name
	Declare    bool
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type Consumer struct {
	Tag       string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type Option func(client *Client)

func WithReplyQueue(rq ReplyQueue) Option {
	return func(client *Client) {
		client.replyQueueOpt = rq
	}
}

func WithConsumer(c Consumer) Option {
	return func(client *Client) {
		client.consumerOpt = c
	}
}

func WithPreFetchCount(count int) Option {
	return func(client *Client) {
		client.preFetchCountOpt = count
	}
}

func WithWorkerCount(count int) Option {
	return func(client *Client) {
		client.workerCountOpt = count
	}
}

func WithShutdownPeriod(d time.Duration) Option {
	return func(client *Client) {
		client.shutdownPeriodOpt = d
	}
}

func WithContext(ctx context.Context) Option {
	return func(client *Client) {
		client.context = ctx
	}
}
