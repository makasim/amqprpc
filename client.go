package amqprpc

import (
	"context"
	"errors"
	"sync"

	"github.com/makasim/amqpextra/publisher"

	"time"

	"fmt"

	"github.com/google/uuid"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/streadway/amqp"
)

var ErrNotDone = errors.New("amqprpc: call is not done")
var ErrReplyQueueGoneAway = errors.New("amqprpc: reply queue has gone away")
var ErrShutdown = errors.New("amqprpc: client is shut down")

type options struct {
	replyQueue ReplyQueue
	consumer   Consumer

	preFetchCount  int
	workerCount    int
	shutdownPeriod time.Duration
}

type replyQueue struct {
	name    string
	closeCh <-chan struct{}
}

type Client struct {
	opts options

	context    context.Context
	cancelFunc context.CancelFunc

	pool *pool

	consumer          *consumer.Consumer
	consumerStateCh   chan consumer.State
	consumerUnreadyCh chan error

	publisher          *publisher.Publisher
	publisherStateCh   chan publisher.State
	publisherUnreadyCh chan error

	replyQueueCh chan replyQueue

	closeCallsCh chan struct{}

	closingMutex sync.Mutex
	closing      bool
}

func New(
	consumerConnCh,
	publisherConnCh <-chan *amqpextra.Connection,
	opts ...Option,
) (*Client, error) {
	c := &Client{
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

		context:            context.Background(),
		closeCallsCh:       make(chan struct{}),
		replyQueueCh:       make(chan replyQueue),
		consumerUnreadyCh:  make(chan error),
		consumerStateCh:    make(chan consumer.State, 1),
		publisherUnreadyCh: make(chan error),
		publisherStateCh:   make(chan publisher.State, 1),
		pool:               newPool(),
	}

	for _, opt := range opts {
		opt(c)
	}

	handler := consumer.Wrap(
		consumer.HandlerFunc(c.reply),
		middleware.Recover(),
		middleware.AckNack(),
	)
	
	var err error

	c.consumer, err = amqpextra.NewConsumer(
		consumerConnCh,
		c.opts.resolveConsumerOptions(handler, c.consumerStateCh)...)
	if err != nil {
		return nil, err
	}

	c.publisher, err = amqpextra.NewPublisher(
		publisherConnCh,
		publisher.WithNotify(c.publisherStateCh))
	if err != nil {
		return nil, err
	}
	
	c.context, c.cancelFunc = context.WithCancel(c.context)

	go c.serveConsumerReplyQueue()
	go c.serveConsumerUnreadyState()
	go c.servePublisherUnreadyState()

	return c, nil
}

func (c *Client) Go(msg publisher.Message, done chan *Call) *Call {
	call := newCall(msg, done, c.pool, c.opts.consumer.AutoAck)
	go c.send(call)

	return call
}

func (c *Client) Call(msg publisher.Message) (amqp.Delivery, error) {
	doneCh := make(chan *Call, 1)
	call := newCall(msg, doneCh, c.pool, c.opts.consumer.AutoAck)
	c.send(call)
	return call.Reply()
}

func (c *Client) Close() error {
	c.closingMutex.Lock()
	if c.closing {
		c.closingMutex.Unlock()
		return ErrShutdown
	}

	c.closing = true
	c.closingMutex.Unlock()

	defer c.cancelFunc()
	defer c.consumer.Close()
	defer c.publisher.Close()

	shutdownPeriodTimer := time.NewTimer(c.opts.shutdownPeriod)
	defer shutdownPeriodTimer.Stop()

	c.publisher.Close()
	select {
	case <-c.publisher.NotifyClosed():
	case <-shutdownPeriodTimer.C:
		return fmt.Errorf("amqprpc: shutdown grace period time out: publisher not stopped")
	}
	var result error
	if c.pool.count() > 0 {
		ticker := time.NewTicker(time.Millisecond * 200)
		defer ticker.Stop()

	loop:
		for {
			select {
			case <-ticker.C:
				if c.pool.count() == 0 {
					break loop
				}
			case <-shutdownPeriodTimer.C:
				result = fmt.Errorf("amqprpc: shutdown grace period time out: some calls have not been done")
				shutdownPeriodTimer.Reset(2 * time.Second)

				break loop
			}
		}
	}

	close(c.closeCallsCh)

	c.consumer.Close()
	select {
	case <-c.consumer.NotifyClosed():
	case <-shutdownPeriodTimer.C:
		return fmt.Errorf("amqprpc: shutdown grace period time out: consumer not stopped")
	}

	return result
}

func (c *Client) send(call *Call) {
	var (
		publisherUnreadyCh chan error
		consumerUnreadyCh  chan error
	)

	if call.request.ErrOnUnready {
		publisherUnreadyCh = c.publisherUnreadyCh
		consumerUnreadyCh = c.consumerUnreadyCh
	}

	if call.request.Context == nil {
		call.request.Context = context.Background()
	}

	select {
	case rq := <-c.replyQueueCh:
		msg := call.Request()
		msg.Publishing.ReplyTo = rq.name
		msg.Publishing.CorrelationId = uuid.New().String()
		msg.ResultCh = make(chan error, 1)
		
		call.set(msg)
		c.pool.set(call)
		
		err := c.publisher.Publish(call.request)
		if err != nil {
			call.errored(err)
			return
		}

		c.waitReply(call, rq)
		return
	case <-call.Closed():
		return
	// noinspection GoNilness
	case err := <-consumerUnreadyCh:
		call.errored(fmt.Errorf("amqprpc: consumer unready: %s", err))
		return
	// noinspection GoNilness
	case err := <-publisherUnreadyCh:
		call.errored(fmt.Errorf("amqprpc: publisher not ready: %s", err))
		return
	case <-call.request.Context.Done():
		call.errored(call.request.Context.Err())
		return
	case <-c.publisher.NotifyClosed():
		call.errored(ErrShutdown)
		return
	}
}

func (c *Client) waitReply(call *Call, rq replyQueue) {
	publishResultCh := call.Request().ResultCh
	
	for {
		select {
		case err := <-publishResultCh:
			if err != nil {
				call.errored(err)
				return
			}

			publishResultCh = nil
			continue
		case <-call.Closed():
			return
		case <-call.closeCh:
			return
		case <-c.closeCallsCh:
			call.errored(ErrShutdown)
			return
		case <-rq.closeCh:
			call.errored(ErrReplyQueueGoneAway)
			return
		case <-call.request.Context.Done():
			call.errored(call.request.Context.Err())
			return
		}
	}
}

func (c *Client) reply(_ context.Context, msg amqp.Delivery) interface{} {
	if msg.CorrelationId == "" {
		return middleware.Nack
	}

	call, ok := c.pool.fetch(msg.CorrelationId)
	if !ok {
		return middleware.Nack
	}

	if !call.ok(msg) {
		return middleware.Nack
	}

	return middleware.Ack
}

func (c *Client) serveConsumerReplyQueue() {
	closeCh := make(chan struct{})
	var localReplyQueueCh chan replyQueue
	var rq replyQueue

	for {
		select {
		case state := <-c.consumerStateCh:
			if state.Unready != nil {
				if rq == (replyQueue{}) {
					continue
				}

				if c.opts.replyQueue.Name == "" || c.opts.replyQueue.AutoDelete{
					localReplyQueueCh = nil
					rq = replyQueue{}

					close(closeCh)
					closeCh = make(chan struct{})
				}

				continue
			}
			
			if state.Ready != nil {
				localReplyQueueCh = c.replyQueueCh
				rq = replyQueue{name: state.Ready.Queue, closeCh: closeCh}
			}

			continue
		case <-c.consumer.NotifyClosed():
			return
		case localReplyQueueCh <- rq:
		}
	}
}

func (c *Client) serveConsumerUnreadyState() {
	localStateCh := c.consumer.Notify(make(chan consumer.State, 1))
	localConsumerUnreadyCh := c.consumerUnreadyCh
	var err error = amqp.ErrClosed

	for {
		select {
		case state, ok := <-localStateCh:
			if !ok {
				panic("that should never happen")
			}

			if state.Ready != nil {
				localConsumerUnreadyCh = nil
			}
			if state.Unready != nil {
				err = state.Unready.Err
				localConsumerUnreadyCh = c.consumerUnreadyCh
			}

			continue
		case localConsumerUnreadyCh <- err:
			continue
		case <-c.context.Done():
			return
		}
	}
}

func (c *Client) servePublisherUnreadyState() {
	localPublisherUnreadyCh := c.publisherUnreadyCh
	var err error = amqp.ErrClosed

	for {
		select {
		case state, ok := <-c.publisherStateCh:
			if !ok {
				panic("that should never happen")
			}
			if state.Ready != nil {
				localPublisherUnreadyCh = nil
			}
			if state.Unready != nil {
				err = state.Unready.Err
				localPublisherUnreadyCh = c.publisherUnreadyCh
			}

			continue
		case localPublisherUnreadyCh <- err:
			continue
		case <-c.context.Done():
			return
		}
	}
}

func (o *options) resolveConsumerOptions(h consumer.Handler, sateCh chan consumer.State) []consumer.Option {
	var (
		ops = []consumer.Option{
			consumer.WithWorker(consumer.NewParallelWorker(o.workerCount)),
			consumer.WithNotify(sateCh),
			consumer.WithQos(o.preFetchCount, false),
			consumer.WithHandler(h),
		}

		declare = o.replyQueue.Declare
		name    = o.replyQueue.Name
	)

	if declare && name == "" {
		ops = append(ops, consumer.WithTmpQueue())
	} else if !declare && name == "" {
		panic("declare flag or queue name for ReplyQueue must be provided in WithReplyQueue")
	}

	if declare && name != "" {
		ops = append(ops, consumer.WithDeclareQueue(
			o.replyQueue.Name,
			o.replyQueue.Durable,
			o.replyQueue.AutoDelete,
			o.replyQueue.Exclusive,
			o.replyQueue.NoWait,
			o.replyQueue.Args,
		))
	}

	if !declare && name != "" {
		ops = append(ops, consumer.WithQueue(name))
	}

	return ops
}
