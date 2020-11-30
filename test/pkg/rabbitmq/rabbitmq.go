package rabbitmq

import (
	"context"
	"encoding/json"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/publisher"

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
	publisherDial, err := amqpextra.NewDialer(amqpextra.WithURL(dsn))
	if err != nil {
		log.Fatal(err)
	}

	pub, err := amqpextra.NewPublisher(publisherDial.ConnectionCh())
	if err != nil {
		panic(err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 20*time.Second)

	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		pub.Publish(publisher.Message{
			Key: msg.ReplyTo,
			Publishing: amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body:          []byte(string(msg.Body)),
			},
		})

		if err = msg.Ack(false); err != nil {
			panic(err)
		}

		return nil
	})

	var queueOp consumer.Option
	if declare {
		queueOp = consumer.WithDeclareQueue(queue, false, true, false, false, amqp.Table{})
	} else {
		queueOp = consumer.WithQueue(queue)
	}

	stateCh := make(chan consumer.State, 1)

	consumerDial, err := amqpextra.NewDialer(amqpextra.WithURL(dsn))
	if err != nil {
		log.Fatal(err)
	}

	c, err := amqpextra.NewConsumer(
		consumerDial.ConnectionCh(),
		consumer.WithContext(ctx),
		consumer.WithHandler(h),
		consumer.WithNotify(stateCh),
		queueOp,
		consumer.WithWorker(consumer.NewParallelWorker(10)),
	)
	if err != nil {
		panic(err)
	}

	return func() {
		cancelFunc()
		publisherDial.Close()
		consumerDial.Close()
		c.Close()
		pub.Close()
	}
}

func RunSleepServer(dsn, queue string, dur time.Duration) func() {
	conn, err := amqpextra.NewDialer(amqpextra.WithURL(dsn))
	if err != nil {
		log.Fatal(err)
	}

	pub, err := amqpextra.NewPublisher(conn.ConnectionCh())
	if err != nil {
		panic(err)
	}

	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		time.Sleep(dur)
		pub.Publish(publisher.Message{
			Key: msg.ReplyTo,
			Publishing: amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body:          msg.Body,
			},
		})

		if err = msg.Ack(false); err != nil {
			log.Fatal(err)
		}

		return nil
	})

	c, err := amqpextra.NewConsumer(
		conn.ConnectionCh(),
		consumer.WithWorker(consumer.NewParallelWorker(10)),
		consumer.WithDeclareQueue(queue, false, true, false, false, amqp.Table{}),
		consumer.WithHandler(h),
	)

	return func() {
		c.Close()
		pub.Close()
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
