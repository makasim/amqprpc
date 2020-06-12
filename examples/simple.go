package main

import (
	"context"

	"time"

	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqprpc"
	"github.com/streadway/amqp"
)

func main() {
	// Init connection properly. For more details visit amqpextra package repository.
	var conn *amqpextra.Connection

	// Application context. most likely controlled by os signals
	ctx := context.Background()

	// In some cases you might want to provide different connections for consumer and publisher.
	client := amqprpc.New(conn, conn)
	go client.Run(ctx)

	reqCtx, reqCancelFunc := context.WithTimeout(ctx, time.Second)
	replyCh := client.Call(reqCtx, amqp.Publishing{
		Body: []byte(`Have you heard the news?`),
	})
	defer reqCancelFunc()

	select {
	case <-reqCtx.Done():
		// No reply with given time.
	case r := <-replyCh:
		if r.Err != nil {
			log.Printf(r.Err.Error())

			return
		}

		log.Print(string(r.Msg.Body))
	case <-ctx.Done():
		// The application is about to stop.
		// In simple scenario just cancel request context to notify RPCClient that you no longer wait for reply
		// In more advanced scenario you can still give some time for the reply to come and only after exit.
		reqCancelFunc()
	}
}
