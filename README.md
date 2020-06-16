# Golang AMQP RPC Client

Features:

* Protocol agnostic RPC Client over AMQP.
* Can simultaneity talk to multiple servers.
* Call cancellation.
* Buffer multiple replies in a channel.
* Client close method wait for calls in flight.

Example:

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
	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqprpc"})

	client, err := amqprpc.New(conn, conn)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	call := client.Go(amqpextra.Publishing{
		Key: "a_queue",
		Message: amqp.Publishing{
			Body: []byte(`Have you heard the news?`),
		},
	}, make(chan *amqprpc.Call, 1))
	defer call.Close()

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

```