package main

import (
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"log"
	"time"
)

// taskPuller pulls Pub/Sub messages on requests from handlers,
// then sends the messages to handlers.
type taskPuller struct {
	subs         subs
	maxExtension time.Duration

	// respCh is the channel to which the pulled messages are sent.
	respCh chan<- *pubsub.Message

	// reqCh is the channel of requests from handlers.
	reqCh <-chan bool

	fetchMsg    fetchMsgFunc
	initMsgIter initMsgIterFunc
}

// makePullerWithDefault makes a puller struct setting the default values.
func makePullerWithDefault(puller taskPuller) *taskPuller {
	puller.initMsgIter = initMsgIter
	puller.fetchMsg = fetchMsg
	return &puller
}

// msgIter is the type of *pubsub.MessageIterator.
type msgIter interface {
	Next() (*pubsub.Message, error)
	Stop()
}

// subs is the type of *pubsub.Subscription
type subs interface {
	Pull(context.Context, ...pubsub.PullOption) (*pubsub.MessageIterator, error)
}

// pullTillShutdown continuously pulls Pub/Sub messages on requests,
// then sends the messages as responses.
// The method returns when the context is shutdown.
func (puller *taskPuller) pullTillShutdown(ctx context.Context) {
	it := puller.initMsgIter(ctx, puller.subs, puller.makePullOpts())
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
		case <-puller.reqCh:
			if msg := puller.fetchMsg(ctx, it); msg != nil {
				puller.respCh <- msg
			} else {
				log.Printf("puller: shutdown while waiting message")
				return
			}
		}
	}
}

// makePullOpts makes pull options.
func (puller *taskPuller) makePullOpts() []pubsub.PullOption {
	return []pubsub.PullOption{pubsub.MaxExtension(puller.maxExtension)}
}

// initMsgIterFunc is the type of initMsgIter function.
type initMsgIterFunc func(context.Context, subs, []pubsub.PullOption) msgIter

// initMsgIter returns a MessageIterator,
// or nil if the context is shutdown.
func initMsgIter(ctx context.Context, subs subs, opts []pubsub.PullOption) msgIter {
	// TODO consider what to do for almost irrecoverable error such as NOTFOUND
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

// fetchMsgFunc is the type of fetchMsg function.
type fetchMsgFunc func(context.Context, msgIter) *pubsub.Message

// fetchMsg fetches a message, or returns nil if the context is shutdown.
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
