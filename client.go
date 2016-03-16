package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/streadway/amqp"
)

type amqpConfiguration struct {
	url      string
	cert     string
	key      string
	caCerts  []string
	exchange string
	queue    string
}

// Message is a container for the payload and command
type Message struct {
	Command string
	Payload string

	delivery *amqp.Delivery
}

// Ack is a pass through to the raw delivery so that the final
// consumer can handle the success/failure decision
func (m Message) Ack(state bool) {
	m.delivery.Ack(state)
}

// Connect returns a connected consumer that you can immediately start
// taking off of the IncomingMessages channel
func Connect(amqpConfig *amqpConfiguration) (<-chan *Message, error) {
	messages := make(chan *Message)

	// first get a TLS connection to the broker
	conn, err := dial(amqpConfig)
	if err != nil {
		return nil, err
	}

	// then connect to the exchange
	deliveries, err := connect(conn, amqpConfig)
	if err != nil {
		return nil, err
	}

	log.Println("Consumer connected and starting to consume")

	// everything "should" be ok - start translating
	go translate(deliveries, messages)
	return messages, nil
}

// responsible for translating between the AMQP delivery and the Message
func translate(deliveries <-chan amqp.Delivery, messages chan<- *Message) {
	for delivery := range deliveries {
		msg := new(Message)
		if err := json.Unmarshal(delivery.Body, msg); err != nil {
			log.Printf("Failed to parse message: %s\n", delivery.Body)
		} else {
			msg.delivery = &delivery
			messages <- msg
		}
	}

	// will be closed when we are out of deliveries (it is closed)
	close(messages)
}

// responsible for contacting the broker with a TLS connection
func dial(config *amqpConfiguration) (*amqp.Connection, error) {
	cfg := new(tls.Config)

	cfg.RootCAs = x509.NewCertPool()
	for _, certPath := range config.caCerts {
		ca, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}

	cert, err := tls.LoadX509KeyPair(config.cert, config.key)
	if err != nil {
		return nil, err
	}
	cfg.Certificates = append(cfg.Certificates, cert)

	log.Printf("Connecting to AMQ: %s\n", config.url)
	return amqp.DialTLS(config.url, cfg)
}

// responsible for setting up the actual queue, and starting to consume
func connect(conn *amqp.Connection, config *amqpConfiguration) (<-chan amqp.Delivery, error) {
	log.Printf("Connected to broker - getting Channel\n")
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = channel.ExchangeDeclare(
		config.exchange, // name
		"fanout",        // kind
		true,            // durable
		true,            // auto delete
		false,           // internal
		false,           // noWait
		nil,             // amqp.Table
	); err != nil {
		return nil, err
	}

	var queue amqp.Queue
	log.Printf("Declaring queue %s\n", config.queue)

	if queue, err = channel.QueueDeclare(
		config.queue, // name of the queue
		true,         // durable
		true,         // delete when usused
		false,        // exclusive
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, err
	}

	if err = channel.QueueBind(
		queue.Name,            // name of the queue
		"bitballoon.commands", // bindingKey
		config.exchange,       // sourceExchange
		false,                 // noWait
		nil,                   // arguments
	); err != nil {
		return nil, err
	}

	log.Printf("Queue bound to Exchange, starting Consumer\n")
	if delivery, err := channel.Consume(
		queue.Name,     // name
		"cache-primer", // consumerTag,
		false,          // noAck
		false,          // exclusive
		false,          // noLocal
		false,          // noWait
		nil,            // arguments
	); err == nil {
		return delivery, nil
	}

	return nil, err
}
