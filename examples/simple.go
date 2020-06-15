package main

import (
	"log"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqprpc"
	"github.com/streadway/amqp"
)

func main() {
	// Init connection properly. For more details visit amqpextra package repository.
	var conn *amqpextra.Connection

	// In some cases you might want to provide different connections for consumer and publisher.
	client, err := amqprpc.New(
		conn,
		conn,
		// add some options
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:    "custom_reply_queue",
			Declare: true,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Do RPC
	call := client.Go(amqpextra.Publishing{
		Key: "a_queue",
		Message: amqp.Publishing{
			Body: []byte(`Have you heard the news?`),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Cancel()

	select {
	case <-call.Done():
		rpl, err := call.Delivery()
		if err != nil {
			log.Fatal(err)

			return
		}

		log.Print(string(rpl.Body))

	case <-time.NewTimer(time.Second).C:
		// timeout
	}
}
