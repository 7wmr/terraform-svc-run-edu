package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/streadway/amqp"
	"log"
)

var msgEndpoint *string
var msgCredentials *string
var dbsEndpoint *string
var dbsCredentials *string

// failOnError - log an error messsage upon failure
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Request - declare structure
type Request struct {
	gorm.Model
	UUID     string `json:"uuid" sql:"not null; size:255; unique"`
	Hostname string `json:"hostname" sql:"not null; size:255"`
}

func main() {
	msgEndpoint = flag.String("msg-endpoint", "", "RabbitMQ messaging endpoint hostname:port")
	msgCredentials = flag.String("msg-credentials", "", "RabbitMQ messaging credentials")
	dbsEndpoint = flag.String("dbs-endpoint", "", "MySQL endpoint hostname:port")
	dbsCredentials = flag.String("dbs-credentials", "", "MySQL credentials")
	flag.Parse()

	baseMysqlString := "%s@tcp(%s)%s?charset=utf8&parseTime=True&loc=Local"

	mysqlString := fmt.Sprintf(
		baseMysqlString,
		*dbsCredentials,
		*dbsEndpoint,
		"")

	db, err := gorm.Open("mysql", mysqlString)
	failOnError(err, "Failed to open instance connection")
	db.Exec("CREATE DATABASE IF NOT EXISTS TerraformEdu;")
	db.Close()

	mysqlString = fmt.Sprintf(
		baseMysqlString,
		*dbsCredentials,
		*dbsEndpoint,
		"/TerraformEdu")

	db, err := gorm.Open("mysql", mysqlString)
	failOnError(err, "Failed to open db connection")
	db.AutoMigrate(&Request{})
	db.Close()

	rabbitMQString := fmt.Sprintf(
		"amqp://%s@%s/",
		*msgCredentials,
		*msgEndpoint)

	conn, err := amqp.Dial(rabbitMQString)

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
		db, err := gorm.Open("mysql", mysqlString)
		failOnError(err, "Failed to open db connection")
		defer db.Close()

		for d := range msgs {
			var request Request
			log.Printf(string(d.Body))
			if err := json.Unmarshal([]byte(d.Body), &request); err != nil {
				log.Fatal(err)
			}
			db.Create(&request)
			d.Ack(true)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
