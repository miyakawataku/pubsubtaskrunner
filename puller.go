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
	ctx            context.Context
	msgCh          chan<- *pubsub.Message
	pullReq        <-chan bool
	fetchMsg       fetchMsgFunc
	initMsgIter    initMsgIterFunc
}

type fetchMsgFunc func(msgIter, context.Context) *pubsub.Message
type initMsgIterFunc func(subs, []pubsub.PullOption, context.Context) msgIter

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

func (puller *taskPuller) pullTillShutdown() {
	subs := puller.psClient.Subscription(puller.subname)
	opts := puller.makePullOps()
	it := puller.initMsgIter(subs, opts, puller.ctx)
	if it == nil {
		log.Printf("puller: shutdown before being initialized")
		return
	}
	defer it.Stop()

	log.Print("puller: initialized")

	for {
		select {
		case <-puller.ctx.Done():
			log.Printf("puller: shutdown while waiting request")
			return
		case <-puller.pullReq:
			if msg := puller.fetchMsg(it, puller.ctx); msg != nil {
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

func initMsgIter(subs subs, opts []pubsub.PullOption, ctx context.Context) msgIter {
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

func fetchMsg(it msgIter, ctx context.Context) *pubsub.Message {
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
