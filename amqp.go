package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func amqp_connection_loop() {

	for {
		err := amqp_connection()
		if err != nil {
			log.Printf("RMQ connection issue: %s", err.Error())
		}
		time.Sleep(3 * time.Second)
	}
}

func amqp_connection() (err error) {
	log.Printf("RMQ amqp_connection")

	useTLS := false
	if rabbitmq_key_file != "" {
		useTLS = true
	}

	userPassword := ""
	protocol := "amqp"
	if useTLS {
		protocol = "amqps"
	} else {

	}
	userPassword = fmt.Sprintf("%s:%s@", rabbitmq_user, rabbitmq_password)
	rmqURL := fmt.Sprintf("%s://%s%s:%s/", protocol, userPassword, rabbitmq_host, rabbitmq_port)

	if useTLS {

		cfg := new(tls.Config)

		// see at the top
		cfg.RootCAs = x509.NewCertPool()

		var ca []byte
		ca, err = ioutil.ReadFile(rabbitmq_cacert_file)
		if err != nil {
			err = fmt.Errorf("could not read file (%s): %s ", rabbitmq_cacert_file, err.Error())
			return
		}

		cfg.RootCAs.AppendCertsFromPEM(ca)

		// Move the client cert and key to a location specific to your application
		// and load them here.
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(rabbitmq_cert_file, rabbitmq_key_file)
		if err != nil {
			err = fmt.Errorf("function LoadX509KeyPair failed: %s ", err.Error())
			return
		}

		cfg.Certificates = append(cfg.Certificates, cert)

		amqp_con, err = amqp.DialTLS(rmqURL, cfg)
	} else {
		amqp_con, err = amqp.Dial(rmqURL)
	}
	if err != nil {
		err = fmt.Errorf("failed to connect to RabbitMQ (%s): %s ", rmqURL, err.Error())
		return
	}
	//defer conn.Close()

	amqp_chan, err = amqp_con.Channel()
	if err != nil {
		err = fmt.Errorf("failed to open a channel: %s", err.Error())
		return
	}
	//defer ch.Close()
	notifyCloseChannel = make(chan *amqp.Error)
	amqp_chan.NotifyClose(notifyCloseChannel)

	//_, err = amqp_chan.QueueDeclare(
	//	rabbitmq_queue, // name
	// 	false,          // durable
	// 	false,          // delete when unused
	// 	false,          // exclusive
	// 	false,          // no-wait
	// 	nil,            // arguments
	// )

	// if err != nil {
	// 	err = fmt.Errorf("failed to declare a queue: %s", err.Error())
	// 	return
	// }

	// err = amqp_chan.QueueBind(rabbitmq_queue, "#", rabbitmq_exchange, false, nil)
	// if err != nil {
	// 	err = fmt.Errorf("failed to bind queue to exchange: %s", err.Error())
	// 	return
	// }

	log.Printf("RMQ connection established")
	err = <-notifyCloseChannel

	log.Printf("RMQ got error: %s", err.Error())
	err = nil

	return
}

func send_amqp_message(username string, msg []byte) (err error) {

	//body := "Hello World!"
	for {
		if amqp_chan == nil {
			log.Printf("waiting on amqp_chan...")
			time.Sleep(time.Second * 3)
			continue
		}
		err = amqp_chan.Publish(
			rabbitmq_exchange,   // exchange
			rabbitmq_routingkey, // routing key
			false,               // mandatory
			false,               // immediate
			amqp.Publishing{
				ContentType:  "application/json", //"text/plain",
				Body:         msg,
				DeliveryMode: 2,
				UserId:       username,
			})

		if err != nil {
			log.Printf("error: %s", err.Error())
			time.Sleep(time.Second * 3)
			continue
		}
		break
	}
	return
}
