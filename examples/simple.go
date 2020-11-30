package main

import (
	"fmt"
	"log"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/makasim/amqprpc"
	"github.com/streadway/amqp"
)

const dsn = "amqp://guest:guest@localhost:5672"

func main() {

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(dsn))
	if err != nil {
		log.Fatal(err)
	}

	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(dsn))
	if err != nil {
		log.Fatal(err)
	}

	consumerConn := consumerDial.ConnectionCh()

	publisherConn := publisherDial.ConnectionCh()

	client, err := amqprpc.New(
		publisherConn,
		consumerConn,
		amqprpc.WithReplyQueue(amqprpc.ReplyQueue{
			Name:    "rpc_queue",
			Declare: false,
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	call := client.Go(publisher.Message{
		ErrOnUnready: false,
		Key:          "a_reply_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`Have you heard the news?`),
		},
	}, make(chan *amqprpc.Call, 1))

	select {
	case <-call.Done():
		rpl, err := call.Delivery()
		if err != nil {
			log.Fatal(err)
			return
		}

		log.Print("DELIVERED: ", string(rpl.Body))

	case <-time.NewTimer(time.Second * 3).C:
		fmt.Println("close")
		call.Close()
	}
}
