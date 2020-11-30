package e2e_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/makasim/amqpextra/declare"

	"github.com/makasim/amqpextra/publisher"

	"github.com/makasim/amqpextra"
	"go.uber.org/goleak"

	"time"

	"context"

	"github.com/makasim/amqprpc"
	"github.com/makasim/amqprpc/test/pkg/assertlog"
	"github.com/makasim/amqprpc/test/pkg/rabbitmq"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	AMQPDSN    = "amqp://guest:guest@rabbitmq:5672/amqprpc"
	InvalidDSN = "amqp://guest:guest@invalid:5672/amqprpc"
)

func TestNoConsumerConnectionNotErroredOnUnready(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()

	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, err)

	call := client.Go(publisher.Message{
		Key: "a_queue",
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	require.NoError(t, client.Close())

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: client is shut down")
}

func TestNoConsumerConnectionContextCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()

	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	call := client.Go(publisher.Message{
		Key:          "foo_queue",
		Context:      ctx,
		ErrOnUnready: false,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	cancelFunc()

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "context canceled")

	require.NoError(t, client.Close())
}

func TestNoConsumerConnectionContextDeadlined(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancelFunc()

	call := client.Go(publisher.Message{
		Key:     "foo_queue",
		Context: ctx,
	}, make(chan *amqprpc.Call, 1))

	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	<-call.Done()
	_, err = call.Delivery()
	consumerDial.Close()
	require.EqualError(t, err, "context deadline exceeded")

	require.NoError(t, client.Close())
}

func TestConsumerConnectionErroredOnUnready(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	publisherConn := publisherDial.ConnectionCh()
	consumerConn := consumerDial.ConnectionCh()

	client, err := amqprpc.New(
		consumerConn,
		publisherConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "a_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	defer client.Close()

	time.Sleep(time.Second)
	call := client.Go(
		publisher.Message{
			Key:          "foo_queue",
			ErrOnUnready: true,
		}, make(chan *amqprpc.Call, 1))

	call = <-call.Done()

	_, err = call.Delivery()

	require.EqualError(t, err, fmt.Sprintf("amqprpc: consumer unready: %s", amqp.ErrClosed))

	require.NoError(t, client.Close())
}

func TestPublisherConnectionErroredOnUnready(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	publisherConn := publisherDial.ConnectionCh()
	consumerConn := consumerDial.ConnectionCh()

	client, err := amqprpc.New(
		consumerConn,
		publisherConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	defer client.Close()
	time.Sleep(time.Second)
	call := <-client.Go(publisher.Message{
		Key:          "foo_queue",
		ErrOnUnready: true,
	}, make(chan *amqprpc.Call, 1)).Done()

	_, err = call.Delivery()

	require.True(t, strings.Contains(err.Error(), "publisher not ready"))

	require.NoError(t, client.Close())
}

func TestNoPublisherConnectionContextCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	call := client.Go(publisher.Message{
		Key:     "foo_queue",
		Context: ctx,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	cancelFunc()

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "context canceled")

	require.NoError(t, client.Close())
}

func TestNoPublisherConnectionContextDeadlined(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancelFunc()

	call := client.Go(publisher.Message{
		Key:          "foo_queue",
		Context:      ctx,
		ErrOnUnready: false,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "context deadline exceeded")

	require.NoError(t, client.Close())
}

func TestNoPublisherConnectionErrorOnUnready(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(InvalidDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}))
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          "foo_queue",
		ErrOnUnready: true,
	}, make(chan *amqprpc.Call, 1))

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, `amqprpc: publisher unready`)

	require.NoError(t, client.Close())
}

func TestCallAndReplyTempReplyQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue, true)()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()

	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:    "",
			Declare: true,
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	timer := time.NewTimer(4 * time.Second)
	defer timer.Stop()

	select {
	case <-call.Done():
		msg, err := call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))

		call.Close()
		msg, err = call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))
	case <-timer.C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}

func TestCallAndReplyCustomReplyQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	replyQueue := rabbitmq.UniqueQueue()

	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue, true)()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:       replyQueue,
			Declare:    true,
			AutoDelete: true,
			Exclusive:  true,
			Args:       amqp.Table{},
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key: rpcQueue,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	timer := time.NewTimer(4 * time.Second)
	defer timer.Stop()

	select {
	case <-call.Done():
		msg, err := call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))

		call.Close()
		msg, err = call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))
	case <-timer.C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}

func TestCancelBeforeReply(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunSleepServer(AMQPDSN, rpcQueue, time.Second)()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:    "",
			Declare: true,
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	time.Sleep(time.Millisecond * 500)
	call.Close()
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call closed")

	time.Sleep(time.Second)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call closed")

	require.NoError(t, client.Close())
}

func TestSendToClosedClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, client.Close())

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	<-call.Done()

	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: client is shut down")
}

func TestShutdownGracePeriodEndedWithAutoDeletedQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(100 * time.Millisecond)

	assert.EqualError(t, client.Close(), "amqprpc: shutdown grace period time out: some calls have not been done")

	<-call.Done()
	_, err = call.Delivery()
	assert.Equal(t, amqprpc.ErrShutdown, err)
}

func TestShutdownGracePeriodEndedWithNoAutoDeleted(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(time.Second),
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:    rabbitmq.UniqueQueue(),
			Declare: true,
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(100 * time.Millisecond)

	assert.EqualError(t, client.Close(), "amqprpc: shutdown grace period time out: some calls have not been done")

	_, err = call.Delivery()
	assert.Equal(t, amqprpc.ErrShutdown, err)
}

func TestShutdownWaitForInflight(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunSleepServer(AMQPDSN, rpcQueue, time.Second)()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(2*time.Second),
	)
	require.NoError(t, err)

	client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: true,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	require.NoError(t, client.Close())
}

func TestErrorReplyQueueHasGoneIfReplyQueueAutoDeleted(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	replyQueue := fmt.Sprintf("reply-queue-%d", time.Now().UnixNano())

	consumerConnName := fmt.Sprintf("amqprpc-consumer-%d", time.Now().UnixNano())
	consumerDial, err := amqpextra.NewDialer(
		amqpextra.WithConnectionProperties(amqp.Table{
			"connection_name": consumerConnName,
		}),
		amqpextra.WithURL(AMQPDSN),
	)
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:       replyQueue,
			AutoDelete: true,
			Declare:    true,
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	assertlog.WaitContainsOrFatal(t, rabbitmq.OpenedConns, consumerConnName, time.Second*10)
	require.True(t, rabbitmq.CloseConn(consumerConnName))

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case <-call.Done():
		_, err := call.Delivery()
		require.Equal(t, err, amqprpc.ErrReplyQueueGoneAway)

		call.Close()
		_, err = call.Delivery()
		require.Equal(t, err, amqprpc.ErrReplyQueueGoneAway)
	case <-timer.C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}

func TestErrorReplyQueueHasGoneIfTemporaryQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()

	consumerConnName := fmt.Sprintf("amqprpc-consumer-%d", time.Now().UnixNano())
	consumerDial, err := amqpextra.NewDialer(
		amqpextra.WithConnectionProperties(amqp.Table{
			"connection_name": consumerConnName,
		}),
		amqpextra.WithURL(AMQPDSN),
	)
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key:          rpcQueue,
		ErrOnUnready: false,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	assertlog.WaitContainsOrFatal(t, rabbitmq.OpenedConns, consumerConnName, time.Second*10)
	require.True(t, rabbitmq.CloseConn(consumerConnName))

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case <-call.Done():
		_, err := call.Delivery()
		require.Equal(t, err, amqprpc.ErrReplyQueueGoneAway)

		call.Close()
		_, err = call.Delivery()
		require.Equal(t, err, amqprpc.ErrReplyQueueGoneAway)
	case <-timer.C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}

func TestCallAndReplyWithNoAutoDeleteQueueAndConsumerLostConnection(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	replyQueue := rabbitmq.UniqueQueue()

	consumerConnName := fmt.Sprintf("amqprpc-consumer-%d", time.Now().UnixNano())
	consumerDial, err := amqpextra.NewDialer(
		amqpextra.WithConnectionProperties(amqp.Table{
			"connection_name": consumerConnName,
		}),
		amqpextra.WithURL(AMQPDSN),
	)
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer publisherDial.Close()

	consumerConn := consumerDial.ConnectionCh()
	publisherConn := publisherDial.ConnectionCh()

	_, err = declare.Queue(context.Background(), consumerDial, rpcQueue, false, false, false, false, amqp.Table{})
	require.NoError(t, err)

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:       replyQueue,
			Declare:    true,
			AutoDelete: false,
			Exclusive:  false,
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(publisher.Message{
		Key: rpcQueue,
		Publishing: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	assertlog.WaitContainsOrFatal(t, rabbitmq.OpenedConns, consumerConnName, 10*time.Second)
	require.True(t, rabbitmq.CloseConn(consumerConnName))

	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue, false)()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-call.Done():
		rpl, err := call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(rpl.Body))

		call.Close()
		_, err = call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(rpl.Body))
	case <-timer.C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}
