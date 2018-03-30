package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	nsq "github.com/nsqio/go-nsq"
)

var (
	nsqflags *flag.FlagSet

	channel = "nsget"
	addr    = "localhost:4150"
	topic   = "nspub"
)

func init() {
	nsqflags = flag.NewFlagSet("", flag.ExitOnError)
	nsqflags.StringVar(&topic, "topic", topic, "NSQ topic name")
	nsqflags.StringVar(&channel, "channel", channel, "NSQ channel name")
	nsqflags.StringVar(&addr, "address", addr, "NSQ tcp address")
}

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	nsqflags.Parse(os.Args[1:])
	config := nsq.NewConfig()
	config.UserAgent = "nsget"
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	consumer.AddHandler(&client{})
	err = consumer.ConnectToNSQD(addr)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}

	<-sigChan
	consumer.Stop()
	<-consumer.StopChan
}

type client struct {
}

func (c *client) HandleMessage(message *nsq.Message) error {
	message.Finish()
	fmt.Printf("%s\n", message.Body)
	return nil
}
