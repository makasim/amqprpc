package rabbitmq

import (
	"context"

	"log"

	"time"

	"fmt"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func UniqueQueue() string {
	return fmt.Sprintf("rpc_server_%d", time.Now().UnixNano())
}

func RunEchoServer(dsn, queue string) func() {
	conn := amqpextra.Dial([]string{dsn})

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := amqpextra.DeclareQueue(ctx, conn, queue, false, true, false, false, amqp.Table{}); err != nil {
		log.Fatal(err)
	}
	defer cancelFunc()

	publisher := conn.Publisher()
	publisher.Start()

	worker := amqpextra.WorkerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		publisher.Publish(amqpextra.Publishing{
			Key: msg.ReplyTo,
			Message: amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body:          msg.Body,
			},
		})

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}

		return nil
	})

	consumer := conn.Consumer(queue, worker)
	consumer.Start()

	<-consumer.Ready()
	<-publisher.Ready()

	return func() {
		consumer.Close()
		publisher.Close()
		conn.Close()
	}
}

func RunSecondSleepServer(dsn, queue string) func() {
	conn := amqpextra.Dial([]string{dsn})

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := amqpextra.DeclareQueue(ctx, conn, queue, false, true, false, false, amqp.Table{}); err != nil {
		log.Fatal(err)
	}
	defer cancelFunc()

	publisher := conn.Publisher()
	publisher.Start()

	worker := amqpextra.WorkerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		time.Sleep(time.Second)

		publisher.Publish(amqpextra.Publishing{
			Key: msg.ReplyTo,
			Message: amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body:          msg.Body,
			},
		})

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}

		return nil
	})

	consumer := conn.Consumer(queue, worker)
	consumer.Start()

	<-consumer.Ready()
	<-publisher.Ready()

	return func() {
		consumer.Close()
		publisher.Close()
		conn.Close()
	}
}
