package rabbitmq

import (
	"context"
	"encoding/json"

	"log"

	"time"

	"fmt"

	"io/ioutil"
	"net/http"
	"net/http/httputil"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func UniqueQueue() string {
	return fmt.Sprintf("amqprpc-unique-queue-%d", time.Now().UnixNano())
}

func RunEchoServer(dsn, queue string, declare bool) func() {
	conn := amqpextra.Dial([]string{dsn})

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	if declare {
		if _, err := amqpextra.DeclareQueue(ctx, conn, queue, false, true, false, false, amqp.Table{}); err != nil {
			log.Fatal(err)
		}
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

func RunSleepServer(dsn, queue string, dur time.Duration) func() {
	conn := amqpextra.Dial([]string{dsn})

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := amqpextra.DeclareQueue(ctx, conn, queue, false, true, false, false, amqp.Table{}); err != nil {
		log.Fatal(err)
	}
	defer cancelFunc()

	publisher := conn.Publisher()
	publisher.Start()

	worker := amqpextra.WorkerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		time.Sleep(dur)

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

func CloseConn(userProvidedName string) bool {
	defer http.DefaultClient.CloseIdleConnections()

	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(OpenedConns()), &data); err != nil {
		panic(err)
	}

	for _, conn := range data {
		connUserProvidedName, ok := conn["user_provided_name"].(string)
		if !ok {
			continue
		}

		if connUserProvidedName == userProvidedName {
			req, err := http.NewRequest(
				"DELETE",
				fmt.Sprintf("http://guest:guest@rabbitmq:15672/api/connections/%s", conn["name"].(string)),
				nil,
			)
			if err != nil {
				panic(err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				panic(err)
			}
			if err := resp.Body.Close(); err != nil {
				panic(err)
			}

			if resp.StatusCode != http.StatusNoContent {
				b, _ := httputil.DumpResponse(resp, true)

				panic(fmt.Sprintf("delete connection request failed:\n\n%s", string(b)))
			}

			return true
		}
	}

	return false
}

func OpenedConns() string {
	req, err := http.NewRequest("GET", "http://guest:guest@rabbitmq:15672/api/connections", nil)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := httputil.DumpResponse(resp, true)

		panic(fmt.Sprintf("get connections request failed:\n\n%s", string(b)))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	return string(b)
}
