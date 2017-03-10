package main

import (
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"log"
	"time"
)

type taskPuller struct {
	psClient       pubsubClient
	subname        string
	commandtimeout time.Duration
	msgCh          chan<- *pubsub.Message
	pullReq        <-chan bool
	fetchMsg       fetchMsgFunc
	initMsgIter    initMsgIterFunc
}

type fetchMsgFunc func(context.Context, msgIter) *pubsub.Message
type initMsgIterFunc func(context.Context, subs, []pubsub.PullOption) msgIter

type pubsubClient interface {
	Subscription(subname string) *pubsub.Subscription
}

type msgIter interface {
	Next() (*pubsub.Message, error)
	Stop()
}

type subs interface {
	Pull(context.Context, ...pubsub.PullOption) (*pubsub.MessageIterator, error)
}

func (puller *taskPuller) pullTillShutdown(ctx context.Context) {
	subs := puller.psClient.Subscription(puller.subname)
	opts := puller.makePullOps()
	it := puller.initMsgIter(ctx, subs, opts)
	if it == nil {
		log.Printf("puller: shutdown before being initialized")
		return
	}
	defer it.Stop()

	log.Print("puller: initialized")

	for {
		select {
		case <-ctx.Done():
			log.Printf("puller: shutdown while waiting request")
			return
		case <-puller.pullReq:
			if msg := puller.fetchMsg(ctx, it); msg != nil {
				puller.msgCh <- msg
			} else {
				log.Printf("puller: shutdown while waiting message")
				return
			}
		}
	}
}

func (puller *taskPuller) makePullOps() []pubsub.PullOption {
	return []pubsub.PullOption{pubsub.MaxExtension(puller.commandtimeout + time.Second*5)}
}

func initMsgIter(ctx context.Context, subs subs, opts []pubsub.PullOption) msgIter {
	for {
		it, err := subs.Pull(ctx, opts...)
		if it != nil {
			return it
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("puller: could not initialize; keep retrying: %v", err)
	}
}

func fetchMsg(ctx context.Context, it msgIter) *pubsub.Message {
	for {
		msg, err := it.Next()
		if msg != nil {
			return msg
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("puller: could not pull message; keep retrying: %v", err)
	}
}
