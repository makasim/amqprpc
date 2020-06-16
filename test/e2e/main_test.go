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

const AMQP_DSN = "amqp://guest:guest@rabbitmq:5672/amqprpc"

func TestNoConsumerConnection(t *testing.T) {
	defer goleak.VerifyNone(t)

	publisherConn := amqpextra.Dial([]string{AMQP_DSN})
	consumerConn := amqpextra.Dial([]string{"amqp://guest:guest@in:5672/amqprpc"})

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name: "foo_reply_queue",
		}))
	require.NoError(t, err)

	call := client.Go(amqpextra.Publishing{
		Key:       "foo_queue",
		WaitReady: true,
	}, make(chan *amqprpc.Call, 1))

	time.Sleep(2 * time.Second)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call is not done")

	call.Close()
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call canceled")

	publisherConn.Close()
	consumerConn.Close()
	require.NoError(t, client.Close())
}

func TestNoPublisherConnectionNoWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	consumerConn := amqpextra.Dial([]string{AMQP_DSN})
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

	time.Sleep(2 * time.Second)
	_, err = call.Delivery()
	require.EqualError(t, err, `Exception (504) Reason: "channel/connection is not open"`)

	call.Close()
	_, err = call.Delivery()
	require.EqualError(t, err, `Exception (504) Reason: "channel/connection is not open"`)

	publisherConn.Close()
	consumerConn.Close()
	require.NoError(t, client.Close())
}

func TestCallAndReplyTempReplyQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunEchoServer(AMQP_DSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQP_DSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQP_DSN})
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
	defer rabbitmq.RunEchoServer(AMQP_DSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQP_DSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQP_DSN})
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
	defer rabbitmq.RunSecondSleepServer(AMQP_DSN, rpcQueue)()

	consumerConn := amqpextra.Dial([]string{AMQP_DSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQP_DSN})
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
	require.EqualError(t, err, "amqprpc: call canceled")

	time.Sleep(time.Second)
	_, err = call.Delivery()
	require.EqualError(t, err, "amqprpc: call canceled")

	require.NoError(t, client.Close())
}

func TestShutdownGracePeriodEnded(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()

	consumerConn := amqpextra.Dial([]string{AMQP_DSN})
	defer consumerConn.Close()
	publisherConn := amqpextra.Dial([]string{AMQP_DSN})
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

	require.EqualError(t, client.Close(), "amqprpc: shutdown grace period time out: some calls have not been done")
}
