package main

import (
	//"encoding/json"
	"flag"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"log"
)

var msgEndpoint *string
var msgCredentials *string

// failOnError - log an error messsage upon failure
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Request - declare structure
type Request struct {
	UUID     uuid.UUID `json:"uuid"`
	Hostname string    `json:"hostname"`
}

func main() {
	msgEndpoint = flag.String("msg-endpoint", "", "RabbitMQ messaging endpoint")
	msgCredentials = flag.String("msg-credentials", "", "RabbitMQ messaging credentials")
	flag.Parse()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s@%s/", *msgCredentials, *msgEndpoint))

	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"Request", // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		50,    // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(string(d.Body))
			d.Ack(true)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
