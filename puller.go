package main

import (
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

type taskPullerConf struct {
	subs         subs
	maxExtension time.Duration

	// respCh is the channel to which the pulled messages are sent.
	respCh chan<- *pubsub.Message

	// reqCh is the channel of requests from handlers.
	reqCh <-chan struct{}

	logger *log.Logger
}

// taskPuller pulls Pub/Sub messages on requests from handlers,
// then sends the messages to handlers.
type taskPuller struct {
	taskPullerConf

	fetchMsg    fetchMsgFunc
	initMsgIter initMsgIterFunc
}

// makePullerWithDefault makes a puller struct setting the default values.
func makePuller(pc taskPullerConf) *taskPuller {
	return &taskPuller{
		taskPullerConf: pc,
		initMsgIter:    initMsgIter,
		fetchMsg:       fetchMsg,
	}
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
	it := puller.initMsgIter(ctx, puller.subs, puller.makePullOpts(), puller.logger)
	if it == nil {
		puller.logger.Printf("puller: shutdown before being initialized")
		return
	}
	defer it.Stop()

	puller.logger.Print("puller: initialized")

	for {
		select {
		case <-ctx.Done():
			puller.logger.Printf("puller: shutdown while waiting request")
			return
		case <-puller.reqCh:
			if msg := puller.fetchMsg(ctx, it, puller.logger); msg != nil {
				puller.respCh <- msg
			} else {
				puller.logger.Printf("puller: shutdown while waiting message")
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
type initMsgIterFunc func(context.Context, subs, []pubsub.PullOption, *log.Logger) msgIter

// initMsgIter returns a MessageIterator,
// or nil if the context is shutdown.
func initMsgIter(ctx context.Context, subs subs, opts []pubsub.PullOption, logger *log.Logger) msgIter {
	// TODO consider what to do for almost irrecoverable error such as NOTFOUND
	for {
		it, err := subs.Pull(ctx, opts...)
		if it != nil {
			return it
		}
		if ctx.Err() != nil {
			return nil
		}
		logger.Printf("puller: could not initialize; keep retrying: %v", err)
	}
}

// fetchMsgFunc is the type of fetchMsg function.
type fetchMsgFunc func(context.Context, msgIter, *log.Logger) *pubsub.Message

// fetchMsg fetches a message, or returns nil if the context is shutdown.
func fetchMsg(ctx context.Context, it msgIter, logger *log.Logger) *pubsub.Message {
	for {
		msg, err := it.Next()
		if msg != nil {
			return msg
		}
		if ctx.Err() != nil {
			return nil
		}
		logger.Printf("puller: could not pull message; keep retrying: %v", err)
	}
}
