package main

import (
	"cloud.google.com/go/pubsub"
	"errors"
	"golang.org/x/net/context"
	"testing"
)

// test puller.pullTillShutdown

func makeFakeInitMsgIter(callCount *int, actions []func() msgIter) initMsgIterFunc {
	return func(context.Context, subs, []pubsub.PullOption) msgIter {
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
	return func(ctx context.Context, it msgIter) *pubsub.Message {
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

func TestPullTillShutdownBreakWhileWaitingRequest(t *testing.T) {
	reqCh := make(chan bool)
	initCallCount := 0
	it := &fakeMessageIterator{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	puller := &taskPuller{
		subs:  new(pubsub.Subscription),
		reqCh: reqCh,
		initMsgIter: makeFakeInitMsgIter(&initCallCount, []func() msgIter{
			func() msgIter { return it },
		}),
	}
	doneCh := make(chan bool)
	go func() {
		puller.pullTillShutdown(ctx)
		doneCh <- true
	}()
	cancel()
	<-doneCh
	if initCallCount != 1 {
		t.Errorf("unexpected initMsgIter call count: got %v, want 1", initCallCount)
	}
}

func TestPullTillShutdownBreakWhileWaitingMessage(t *testing.T) {
	respCh := make(chan *pubsub.Message, 3)
	reqCh := make(chan bool, 3)
	initCallCount := 0
	fetchCallCount := 0
	it := &fakeMessageIterator{}
	msg1 := new(pubsub.Message)
	msg2 := new(pubsub.Message)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	puller := &taskPuller{
		subs:   new(pubsub.Subscription),
		respCh: respCh,
		reqCh:  reqCh,
		initMsgIter: makeFakeInitMsgIter(&initCallCount, []func() msgIter{
			func() msgIter { return it },
		}),
		fetchMsg: makeFakeFetchMsg(&fetchCallCount, []func() *pubsub.Message{
			func() *pubsub.Message { return msg1 },
			func() *pubsub.Message { return msg2 },
			func() *pubsub.Message { cancel(); return nil },
		}),
	}
	doneCh := make(chan bool)
	go func() {
		puller.pullTillShutdown(ctx)
		doneCh <- true
	}()
	reqCh <- true
	reqCh <- true
	reqCh <- true
	resMsg1 := <-respCh
	resMsg2 := <-respCh
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
	if !it.stopCalled {
		t.Errorf("it Stop() not called")
	}
}

func TestPullTillShutdownBreakBeforeInitialized(t *testing.T) {
	initCallCount := 0
	ctx, cancel := context.WithCancel(context.Background())
	puller := &taskPuller{
		subs: new(pubsub.Subscription),
		initMsgIter: makeFakeInitMsgIter(&initCallCount, []func() msgIter{
			func() msgIter { cancel(); return nil },
		}),
	}
	defer cancel()
	doneCh := make(chan bool)
	go func() {
		puller.pullTillShutdown(ctx)
		doneCh <- true
	}()
	<-doneCh
	if initCallCount != 1 {
		t.Errorf("unexpected initMsgIter call count: got %v, want 1", initCallCount)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resIt := initMsgIter(ctx, subs, opts)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resIt := initMsgIter(ctx, subs, opts)
	if resIt != it {
		t.Errorf("initMsgIter it: got: %v, want: %v", resIt, it)
	}
	if subs.callCount != 3 {
		t.Errorf("unexpected call count: got %v, want: 3", subs.callCount)
	}
}

func TestInitMsgIterShutdownOnCancel(t *testing.T) {
	canceledErr := errors.New("canceled")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subs := &fakeSubs{
		actions: []pullFunc{
			func() (*pubsub.MessageIterator, error) {
				cancel()
				return nil, canceledErr
			},
		},
	}
	opts := []pubsub.PullOption{}
	resIt := initMsgIter(ctx, subs, opts)
	if resIt != nil {
		t.Errorf("initMsgIter it: got %v, want: nil", resIt)
	}
	if subs.callCount != 1 {
		t.Errorf("unexpected call count: got %v, want: 1", subs.callCount)
	}
}

func TestInitMsgIterReturnIterIfNoErrorEvenAfterCancel(t *testing.T) {
	it := new(pubsub.MessageIterator)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subs := &fakeSubs{
		actions: []pullFunc{
			func() (*pubsub.MessageIterator, error) {
				cancel()
				return it, nil
			},
		},
	}
	opts := []pubsub.PullOption{}
	resIt := initMsgIter(ctx, subs, opts)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := new(pubsub.Message)
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) { return msg, nil },
		},
	}
	resMsg := fetchMsg(ctx, it)
	if resMsg != msg {
		t.Errorf("fetchMsg msg: got: %v, want: %v", resMsg, msg)
	}
	if it.callCount != 1 {
		t.Errorf("unexpected call count: got: %v, want: 1", it.callCount)
	}
}

func TestFetchMsgRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := new(pubsub.Message)
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) { return nil, errors.New("1st") },
			func() (*pubsub.Message, error) { return nil, errors.New("2nd") },
			func() (*pubsub.Message, error) { return msg, nil },
		},
	}
	resMsg := fetchMsg(ctx, it)
	if resMsg != msg {
		t.Errorf("fetchMsg msg: got: %v, want: %v", resMsg, msg)
	}
	if it.callCount != 3 {
		t.Errorf("unexpected call count: got: %v, want: 3", it.callCount)
	}
}

func TestFetchMsgShutdownOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceledErr := errors.New("canceled")
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) {
				cancel()
				return nil, canceledErr
			},
		},
	}
	resMsg := fetchMsg(ctx, it)
	if resMsg != nil {
		t.Errorf("fetchMsg msg: got: %v, want: nil", resMsg)
	}
	if it.callCount != 1 {
		t.Errorf("fetchMsg call count: %v, want: 1", it.callCount)
	}
}

func TestFetchMsgReturnMsgIfNoErrorEvenAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := new(pubsub.Message)
	it := &fakeMessageIterator{
		actions: []nextFunc{
			func() (*pubsub.Message, error) {
				cancel()
				return msg, nil
			},
		},
	}
	resMsg := fetchMsg(ctx, it)
	if resMsg != msg {
		t.Errorf("fetchMsg msg: got: %v, want: %v", resMsg, msg)
	}
	if it.callCount != 1 {
		t.Errorf("unexpected call count: got: %v, want: 1", it.callCount)
	}
}
