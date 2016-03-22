package lib

import "log"

// NewConsumer returns a connected consumer that you can immediately start
// taking off of the IncomingMessages channel
func NewConsumer(amqpConfig *AMQPConfiguration) (<-chan *Message, error) {
	messages := make(chan *Message)

	// first get a TLS connection to the broker
	conn, err := Dial(amqpConfig.URL, &amqpConfig.TLSConfig)
	if err != nil {
		return nil, err
	}

	// then connect to the exchange
	deliveries, err := Connect(conn, &amqpConfig.Exchange, amqpConfig.Queue)
	if err != nil {
		return nil, err
	}

	log.Println("Consumer connected and starting to consume")

	// everything "should" be ok - start translating
	go translate(deliveries, messages)
	return messages, nil
}
