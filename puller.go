package main

import (
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"log"
	"time"
)

type taskPuller struct {
	psClient       *pubsub.Client
	subname        string
	commandtimeout time.Duration
	ctx            context.Context
	msgCh          chan<- *pubsub.Message
	tokenCh        <-chan bool
}

func (puller *taskPuller) initMessageIterator() *pubsub.MessageIterator {
	subs := puller.psClient.Subscription(puller.subname)
	for {
		it, err := subs.Pull(puller.ctx, pubsub.MaxExtension(puller.commandtimeout+time.Second*5))
		switch {
		case puller.ctx.Err() != nil:
			return nil
		case err != nil:
			log.Printf("puller: could not initialize; keep retrying: %v", err)
		default:
			return it
		}
	}
}

func (puller *taskPuller) pullTillStop() {
	it := puller.initMessageIterator()
	if it == nil {
		log.Printf("puller: shutdown before being initialized")
		return
	}

	log.Print("puller: initialized")

	defer it.Stop()
	for {
		<-puller.tokenCh
		var msg *pubsub.Message
		for msg == nil {
			var err error
			msg, err = it.Next()
			if puller.ctx.Err() != nil {
				log.Printf("puller: shutdown")
				return
			}
			if err != nil {
				log.Printf("puller: could not pull message; keep retrying: %v", err)
				continue
			}
		}
		puller.msgCh <- msg
	}
}
