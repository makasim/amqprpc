# Golang AMQP RPC Client

```go
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
	client := amqprpc.New(conn, conn)
	go client.Run()

	// Client has some configuration properties.
	// By default client creates a temporary queue, but you can provide a custom queue.
	// client.ReplyQueue = "custom_reply_queue"

	// Do RPC
	call := client.Go(amqpextra.Publishing{
		Key: "a_queue",
		Message: amqp.Publishing{
			Body: []byte(`Have you heard the news?`),
		},
	}, make(chan *amqprpc.Call, 1))

	select {
	case <-call.Done():
		log.Print(string(call.Delivery().Body))
	case <-time.NewTimer(time.Second).C:
		call.Cancel()
	}
}

```