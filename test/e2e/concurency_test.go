package e2e_test

import (
	"fmt"
	"testing"

	"github.com/makasim/amqpextra/publisher"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqprpc"
	"github.com/makasim/amqprpc/test/pkg/rabbitmq"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TODO
const LOCALDSN = "amqp://guest:guest@localhost:5672"

func TestSendManyOneByOne(t *testing.T) {
	defer goleak.VerifyNone(t)

	rpcQueue := rabbitmq.UniqueQueue()
	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue, true)()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)
	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	if err != nil {
		panic(err)
	}
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

	calls := make(chan *amqprpc.Call, 2000)

	for i := 0; i < 2000; i++ {
		client.Go(publisher.Message{
			Key:          rpcQueue,
			ErrOnUnready: false,
			Publishing: amqp.Publishing{
				Body: []byte("hello!"),
			},
		}, calls)
	}

	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()

	got := 0
	for {
		select {
		case <-timer.C:
			t.Errorf("waiting for replies timeout")
			return
		case <-calls:
			fmt.Println(got)
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
	defer rabbitmq.RunEchoServer(AMQPDSN, rpcQueue, true)()

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	require.NoError(t, err)

	defer consumerDial.Close()

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(AMQPDSN))
	if err != nil {
		panic(err)
	}
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

	calls := make(chan *amqprpc.Call, 2000)

	for i := 0; i < 5; i++ {
		go func() {
			for i := 0; i < 500; i++ {
				client.Go(publisher.Message{
					Key:          rpcQueue,
					ErrOnUnready: false,
					Publishing: amqp.Publishing{
						Body: []byte("hello!"),
					},
				}, calls)
			}
		}()
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	got := 0
loop:
	for {
		select {
		case <-timer.C:
			t.Errorf("waiting for replies timeout")

			return
		case <-calls:
			got++
			if got == 2000 {
				break loop
			}
		}
	}

	require.NoError(t, client.Close())
}
