package e2e_test

import (
	"testing"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqprpc"
	"github.com/makasim/amqprpc/test/pkg/rabbitmq"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSendManyOneByOne(t *testing.T) {
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

	calls := make(chan *amqprpc.Call, 2000)

	for i := 0; i < 2000; i++ {
		client.Go(amqpextra.Publishing{
			Key:       rpcQueue,
			WaitReady: true,
			Message: amqp.Publishing{
				Body: []byte("hello!"),
			},
		}, calls)
	}

	timer := time.NewTimer(5 * time.Second)
	got := 0
	for {
		select {
		case <-timer.C:
			t.Errorf("waiting for replies timeout")

			return
		case <-calls:
			got++
			if got == 2000 {
				return
			}
		}
	}
}

func TestSendManyConcurrently(t *testing.T) {
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

	calls := make(chan *amqprpc.Call, 2000)

	for i := 0; i < 5; i++ {
		go func() {
			for i := 0; i < 500; i++ {
				client.Go(amqpextra.Publishing{
					Key:       rpcQueue,
					WaitReady: true,
					Message: amqp.Publishing{
						Body: []byte("hello!"),
					},
				}, calls)
			}
		}()
	}

	timer := time.NewTimer(5 * time.Second)
	got := 0
	for {
		select {
		case <-timer.C:
			t.Errorf("waiting for replies timeout")

			return
		case <-calls:
			got++
			if got == 2000 {
				return
			}
		}
	}
}
