package amqprpc

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type ReplyQueue struct {
	Name       string
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
		client.opts.replyQueue = rq
	}
}

func WithConsumer(c Consumer) Option {
	return func(client *Client) {
		client.opts.consumer = c
	}
}

func WithPreFetchCount(count int) Option {
	return func(client *Client) {
		client.opts.preFetchCount = count
	}
}

func WithWorkerCount(count int) Option {
	return func(client *Client) {
		client.opts.workerCount = count
	}
}

func WithShutdownPeriod(d time.Duration) Option {
	return func(client *Client) {
		client.opts.shutdownPeriod = d
	}
}

func WithContext(ctx context.Context) Option {
	return func(client *Client) {
		client.context = ctx
	}
}
