package e2e_test

import (
	"testing"

	"github.com/makasim/amqpextra"
	"go.uber.org/goleak"

	"time"

	"context"

	"github.com/makasim/amqprpc"
	"github.com/makasim/amqprpc/test/pkg/rabbitmq"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
)

const AMQPDSN = "amqp://guest:guest@rabbitmq:5672/amqprpc"

func TestNoConsumerConnectionWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	consumerConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		WaitReady: true,
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

	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	consumerConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

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

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		Context:   ctx,
		WaitReady: true,
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

	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	consumerConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

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

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		Context:   ctx,
		WaitReady: true,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "context deadline exceeded")

	require.NoError(t, client.Close())
}

func TestNoConsumerConnectionNoWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	<-publisherConn.Ready()

	consumerConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		WaitReady: false,
	}, make(chan *amqprpc.Call, 1))

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: consumer unready")

	require.NoError(t, client.Close())
}

func TestNoPublisherConnectionWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	<-consumerConn.Ready()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}),
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		WaitReady: true,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	require.NoError(t, client.Close())

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: client is shut down")
}

func TestNoPublisherConnectionContextCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	<-consumerConn.Ready()

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

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		Context:   ctx,
		WaitReady: true,
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

	publisherConn := amqpextra.Dial([]string{"amqp://guest:guest@invalid:5672/amqprpc"})

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	<-consumerConn.Ready()

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

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		Context:   ctx,
		WaitReady: true,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(500 * time.Millisecond)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, "context deadline exceeded")

	require.NoError(t, client.Close())
}

func TestNoPublisherConnectionNoWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	<-consumerConn.Ready()

	publisherConn := amqpextra.Dial([]string{"amqp://guest:guest@in:5672/amqprpc"})

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}))
	require.NoError(t, err)

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		WaitReady: false,
	}, make(chan *amqprpc.Call, 1))

	<-call.Done()
	_, err = call.Delivery()
	require.EqualError(t, err, `amqprpc: publisher unready`)

	require.NoError(t, client.Close())
}

func TestCallAndReplyTempReplyQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	defer publisherConn.Close()

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

	call := client.Go(amqpextra.Publishing{
		Key:       rpcQueue,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	select {
	case <-call.Done():
		msg, err := call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))

		call.Close()
		msg, err = call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))
	case <-time.NewTimer(4 * time.Second).C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}

func TestCallAndReplyCustomReplyQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	defer publisherConn.Close()

	_, err := amqpextra.DeclareQueue(
		context.Background(),
		consumerConn,
		"rpc_reply_queue",
		false,
		true,
		true,
		false,
		amqp.Table{},
	)
	require.NoError(t, err)

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:    "rpc_reply_queue",
			Declare: false,
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	call := client.Go(amqpextra.Publishing{
		Key:       rpcQueue,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

	select {
	case <-call.Done():
		msg, err := call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))

		call.Close()
		msg, err = call.Delivery()
		require.NoError(t, err)
		require.Equal(t, "hello!", string(msg.Body))
	case <-time.NewTimer(4 * time.Second).C:
		call.Close()
		t.Errorf("call time out")
	}
	require.NoError(t, client.Close())
}

func TestCancelBeforeReply(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunSecondSleepServer(AMQPDSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	defer publisherConn.Close()

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

	call := client.Go(amqpextra.Publishing{
		Key:       rpcQueue,
		WaitReady: true,
		Message: amqp.Publishing{
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

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	defer consumerConn.Close()

	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	defer publisherConn.Close()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, client.Close())

	call := client.Go(amqpextra.Publishing{
		Key:       rpcQueue,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	<-call.Done()

	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: client is shut down")
}

func TestShutdownGracePeriodEnded(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	defer consumerConn.Close()

	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	defer publisherConn.Close()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(time.Second),
	)
	require.NoError(t, err)

	client.Go(amqpextra.Publishing{
		Key:       rpcQueue,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(100 * time.Millisecond)

	require.EqualError(t, client.Close(), "amqprpc: shutdown grace period time out: some calls have not been done")
}

func TestShutdownWaitForInflight(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunSecondSleepServer(AMQPDSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQPDSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQPDSN})
	defer publisherConn.Close()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithShutdownPeriod(2*time.Second),
	)
	require.NoError(t, err)

	client.Go(amqpextra.Publishing{
		Key:       rpcQueue,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte("hello!"),
		},
	}, make(chan *amqprpc.Call, 1))

	require.NoError(t, client.Close())
}
