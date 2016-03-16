package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"

	"github.com/streadway/amqp"
)

// AMQPConfiguration is what it says
type AMQPConfiguration struct {
	URL       string           `json:"url"`
	Exchange  string           `json:"exchange"`
	Queue     string           `json:"queue"`
	TLSConfig TLSConfiguration `json:"tls_config"`
}

// TLSConfiguration contains the configuration for doing TLS
type TLSConfiguration struct {
	Cert    string   `json:"cert"`
	Key     string   `json:"key"`
	CACerts []string `json:"ca_cert"`
}

// IsValid checks for blanks and obvious errors
func (t TLSConfiguration) IsValid() bool {
	if t.Cert == "" {
		return false
	}

	if t.Key == "" {
		return false
	}

	if t.CACerts == nil {
		return false
	}
	return true
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

// Dial is responsible for contacting the broker with a TLS connection
func dial(url string, config *TLSConfiguration) (*amqp.Connection, error) {
	if !config.IsValid() {
		return nil, errors.New("the TLS configuration is invalid")
	}

	cfg := new(tls.Config)

	cfg.RootCAs = x509.NewCertPool()
	for _, certPath := range config.CACerts {
		ca, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}

	cert, err := tls.LoadX509KeyPair(config.Cert, config.Key)
	if err != nil {
		return nil, err
	}
	cfg.Certificates = append(cfg.Certificates, cert)

	log.Printf("Connecting to AMQ: %s\n", url)
	return amqp.DialTLS(url, cfg)
}

// responsible for setting up the actual queue, and starting to consume
func connect(conn *amqp.Connection, config *AMQPConfiguration) (<-chan amqp.Delivery, error) {
	log.Printf("Connected to broker - getting Channel\n")
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = channel.ExchangeDeclare(
		config.Exchange, // name
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
	log.Printf("Declaring queue %s\n", config.Queue)

	if queue, err = channel.QueueDeclare(
		config.Queue, // name of the queue
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
		config.Exchange,       // sourceExchange
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
