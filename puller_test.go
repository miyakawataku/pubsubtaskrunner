package main

import (
	"cloud.google.com/go/pubsub"
	"errors"
	"golang.org/x/net/context"
	"testing"
	"time"
)

// test puller.pullTillShutdown

type fakePsClient struct {
	subs *pubsub.Subscription
}

func (psClient *fakePsClient) Subscription(subname string) *pubsub.Subscription {
	return psClient.subs
}

func makeFakeInitMsgIter(callCount *int, actions []func() msgIter) initMsgIterFunc {
	return func(subs, []pubsub.PullOption, context.Context) msgIter {
		index := *callCount
		*callCount += 1
		if *callCount > len(actions) {
			return nil
		} else {
			action := actions[index]
			return action()
		}
	}
}

func makeFakeFetchMsg(callCount *int, actions []func() *pubsub.Message) fetchMsgFunc {
	return func(it msgIter, ctx context.Context) *pubsub.Message {
		index := *callCount
		*callCount += 1
		if *callCount > len(actions) {
			return nil
		} else {
			action := actions[index]
			return action()
		}
	}
}

func TestPullTillShutdown(t *testing.T) {
	msgCh := make(chan *pubsub.Message, 3)
	pullReq := make(chan bool, 3)
	initCallCount := 0
	fetchCallCount := 0
	it := &fakeMessageIterator{}
	msg1 := new(pubsub.Message)
	msg2 := new(pubsub.Message)
	ctx, cancel := context.WithCancel(context.Background())
	puller := &taskPuller{
		psClient:       &fakePsClient{new(pubsub.Subscription)},
		subname:        "subs",
		commandtimeout: time.Second * 10,
		ctx:            ctx,
		msgCh:          msgCh,
		pullReq:        pullReq,
		initMsgIter: makeFakeInitMsgIter(&initCallCount, []func() msgIter{
			func() msgIter { return it },
		}),
		fetchMsg: makeFakeFetchMsg(&fetchCallCount, []func() *pubsub.Message{
			func() *pubsub.Message { return msg1 },
			func() *pubsub.Message { return msg2 },
			func() *pubsub.Message {
				cancel()
				return nil
			},
		}),
	}
	doneCh := make(chan bool)
	go func() {
		puller.pullTillShutdown()
		doneCh <- true
	}()
	pullReq <- true
	pullReq <- true
	pullReq <- true
	resMsg1 := <-msgCh
	resMsg2 := <-msgCh
	<-doneCh
	if initCallCount != 1 {
		t.Errorf("unexpected initMsgIter call count: got %v, want 1", initCallCount)
	}
	if fetchCallCount != 3 {
		t.Errorf("unexpected fetchMsg call count: got %v, want 3", fetchCallCount)
	}
	if resMsg1 != msg1 {
		t.Errorf("unmatching msg1: %v", resMsg1)
	}
	if resMsg2 != msg2 {
		t.Errorf("unmatching msg2: %v", resMsg2)
	}
}

// test initMsgIter

type pullFunc func() (*pubsub.MessageIterator, error)

type fakeSubs struct {
	actions   []pullFunc
	callCount int
}

func (subs *fakeSubs) Pull(ctx context.Context, opts ...pubsub.PullOption) (*pubsub.MessageIterator, error) {
	index := subs.callCount
	subs.callCount += 1
	if subs.callCount > len(subs.actions) {
		return nil, errors.New("unexpected call")
	}
	action := subs.actions[index]
	return action()
}

func TestInitMsgIter(t *testing.T) {
	it := new(pubsub.MessageIterator)
	subs := &fakeSubs{
		actions: []pullFunc{
			func() (*pubsub.MessageIterator, error) { return it, nil },
		},
	}
	opts := []pubsub.PullOption{}
	ctx, _ := context.WithCancel(context.Background())
	resIt := initMsgIter(subs, opts, ctx)
	if resIt != it {
		t.Errorf("initMsgIter it: got: %v, want: %v", resIt, it)
	}
	if subs.callCount != 1 {
		t.Errorf("unexpected call count: got %v, want: 1", subs.callCount)
	}
}

func TestInitMsgIterRetry(t *testing.T) {
	it := new(pubsub.MessageIterator)
	subs := &fakeSubs{
		actions: []pullFunc{
			func() (*pubsub.MessageIterator, error) { return nil, errors.New("1st") },
			func() (*pubsub.MessageIterator, error) { return nil, errors.New("2nd") },
			func() (*pubsub.MessageIterator, error) { return it, nil },
		},
	}
	opts := []pubsub.PullOption{}
	ctx, _ := context.WithCancel(context.Background())
	resIt := initMsgIter(subs, opts, ctx)
	if resIt != it {
		t.Errorf("initMsgIter it: got: %v, want: %v", resIt, it)
	}
	if subs.callCount != 3 {
		t.Errorf("unexpected call count: got %v, want: 3", subs.callCount)
	}
}

func TestinitMsgIterShutdownOnCancel(t *testing.T) {
	canceledErr := errors.New("canceled")
	ctx, cancel := context.WithCancel(context.Background())
	subs := &fakeSubs{
		actions: []pullFunc{
			func() (*pubsub.MessageIterator, error) {
				cancel()
				return nil, canceledErr
			},
		},
	}
	opts := []pubsub.PullOption{}
	resIt := initMsgIter(subs, opts, ctx)
	if resIt != nil {
		t.Errorf("initMsgIter it: got %v, want: nil", resIt)
	}
	if subs.callCount != 1 {
		t.Errorf("unexpected call count: got %v, want: 1", subs.callCount)
	}
}

func TestinitMsgIterReturnIterIfNoErrorEvenAfterCancel(t *testing.T) {
	it := new(pubsub.MessageIterator)
	ctx, cancel := context.WithCancel(context.Background())
	subs := &fakeSubs{
		actions: []pullFunc{
			func() (*pubsub.MessageIterator, error) {
				cancel()
				return it, nil
			},
		},
	}
	opts := []pubsub.PullOption{}
	resIt := initMsgIter(subs, opts, ctx)
	if resIt != it {
		t.Errorf("initMsgIter it: got: %v, want: %v", resIt, it)
	}
	if subs.callCount != 1 {
		t.Errorf("unexpected call count: got %v, want: 1", subs.callCount)
	}
}

// test fetchMsg

type nextFunc func() (*pubsub.Message, error)

type fakeMessageIterator struct {
	actions    []nextFunc
	callCount  int
	stopCalled bool
}

func (it *fakeMessageIterator) Next() (*pubsub.Message, error) {
	index := it.callCount
	it.callCount += 1
	if it.callCount > len(it.actions) {
		return nil, errors.New("unexpected call")
	}
	action := it.actions[index]
	return action()
}

func (it *fakeMessageIterator) Stop() {
	it.stopCalled = true
}

func TestFetchMsg(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	msg := new(pubsub.Message)
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) { return msg, nil },
		},
	}
	resMsg := fetchMsg(it, ctx)
	if resMsg != msg {
		t.Errorf("fetchMsg msg: got: %v, want: %v", resMsg, msg)
	}
	if it.callCount != 1 {
		t.Errorf("unexpected call count: got: %v, want: 1", it.callCount)
	}
}

func TestFetchMsgRetry(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	msg := new(pubsub.Message)
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) { return nil, errors.New("1st") },
			func() (*pubsub.Message, error) { return nil, errors.New("2nd") },
			func() (*pubsub.Message, error) { return msg, nil },
		},
	}
	resMsg := fetchMsg(it, ctx)
	if resMsg != msg {
		t.Errorf("fetchMsg msg: got: %v, want: %v", resMsg, msg)
	}
	if it.callCount != 3 {
		t.Errorf("unexpected call count: got: %v, want: 3", it.callCount)
	}
}

func TestFetchMsgShutdownOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	canceledErr := errors.New("canceled")
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) {
				cancel()
				return nil, canceledErr
			},
		},
	}
	resMsg := fetchMsg(it, ctx)
	if resMsg != nil {
		t.Errorf("fetchMsg msg: got: %v, want: nil", resMsg)
	}
	if it.callCount != 1 {
		t.Errorf("fetchMsg call count: %v, want: 1", it.callCount)
	}
}

func TestFetchMsgReturnMsgIfNoErrorEvenAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	msg := new(pubsub.Message)
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) {
				cancel()
				return msg, nil
			},
		},
	}
	resMsg := fetchMsg(it, ctx)
	if resMsg != msg {
		t.Errorf("fetchMsg msg: got: %v, want: %v", resMsg, msg)
	}
	if it.callCount != 1 {
		t.Errorf("unexpected call count: got: %v, want: 1", it.callCount)
	}
}
