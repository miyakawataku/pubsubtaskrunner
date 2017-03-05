package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
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
			log.Printf("could not initialize puller; keep retrying: %v", err)
		default:
			return it
		}
	}
}

func (puller *taskPuller) pullTillStop() {
	it := puller.initMessageIterator()
	if it == nil {
		log.Printf("shutdown puller before initialized")
		return
	}

	log.Print("puller has been initialized")

	defer it.Stop()
	for {
		<-puller.tokenCh
		msg, err := it.Next()
		if puller.ctx.Err() != nil {
			log.Printf("shutdown puller")
			break
		}
		if err != nil {
			log.Printf("could not pull message; keep retrying: %v", err)
			continue
		}
		puller.msgCh <- msg
	}
}

type taskHandler struct {
	id             string
	command        string
	args           []string
	commandtimeout time.Duration
	retrytimeout   time.Duration
	ctx            context.Context
	msgCh          <-chan *pubsub.Message
	tokenCh        chan<- bool
	doneCh         chan bool
	tasklogname    string
	maxtasklogkb   int
}

func (handler *taskHandler) handleTasks() {
	for {
		handler.tokenCh <- true
		select {
		case msg := <-handler.msgCh:
			handler.handleSingleTask(msg)
		case <-handler.ctx.Done():
			log.Printf("shutdown %s", handler.id)
			handler.doneCh <- true
			return
		}
	}
}

func (handler *taskHandler) rotateLogIfNecessary() {
	stat, err := os.Stat(handler.tasklogname)
	switch {
	case os.IsNotExist(err):
		log.Printf("create new task log %s", handler.tasklogname)
		return
	case err != nil:
		log.Printf("cannot stat %s: %v", handler.tasklogname, err)
		return
	}

	if stat.Size() >= int64(handler.maxtasklogkb*1000) {
		prevLogName := handler.tasklogname + ".prev"
		log.Printf("mv task log %s to %s", handler.tasklogname, prevLogName)
		if err := os.Rename(handler.tasklogname, prevLogName); err != nil {
			log.Printf("could not mv task log %s to %s: %v", handler.tasklogname, prevLogName, err)
		}
	}
}

func (handler *taskHandler) handleSingleTask(msg *pubsub.Message) {
	retryDeadline := msg.PublishTime.Add(handler.retrytimeout)
	now := time.Now()
	if now.After(retryDeadline) {
		log.Printf("ack message %s because of exceeded retry deadline %v",
			msg.ID, retryDeadline)
		msg.Done(true)
		return
	}

	handler.rotateLogIfNecessary()
	tlfile, err := os.OpenFile(handler.tasklogname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("could not open task log %s: %v", handler.tasklogname, err)
		msg.Done(false)
		return
	}
	defer tlfile.Close()

	log.Printf("process message %s", msg.ID)
	cmd := exec.Command(handler.command, handler.args...)
	cmd.Stdout = tlfile
	cmd.Stderr = tlfile
	cmd.Stdin = bytes.NewReader(msg.Data)
	if err := cmd.Start(); err != nil {
		log.Printf("could not spawn command for message %s: %v", msg.ID, err)
		msg.Done(false)
		return
	}

	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()
	select {
	case <-time.After(handler.commandtimeout):
		if err := cmd.Process.Kill(); err != nil {
			log.Fatal("failed to kill command for command %s: ", msg.ID, err)
		}
		log.Printf("process killed as command timeout reached for message %s", msg.ID)
	case err := <-cmdDone:
		if err != nil {
			msg.Done(false)
			log.Printf("command failed for message %s: %v", msg.ID, err)
		} else {
			msg.Done(true)
			log.Printf("command done for message %s", msg.ID)
		}
	}
}

func newPsClient(opt opt, ctx context.Context) (*pubsub.Client, error) {
	clientOpts := option.WithServiceAccountFile(opt.credentials)
	return pubsub.NewClient(ctx, opt.project, clientOpts)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(fmt.Sprintf("pubsubtaskrunner: pid=%d: ", os.Getpid()))
	opt := parseOpt()

	appCtx, cancelApp := context.WithCancel(context.Background())
	msgCh := make(chan *pubsub.Message)
	tokenCh := make(chan bool, opt.parallelism)

	// start puller
	psClient, err := newPsClient(opt, appCtx)
	if err != nil {
		log.Fatalf("could not make Pub/Sub client: %v", err)
	}
	puller := &taskPuller{
		psClient:       psClient,
		subname:        opt.subscription,
		commandtimeout: opt.commandtimeout,
		ctx:            appCtx,
		msgCh:          msgCh,
		tokenCh:        tokenCh,
	}
	go puller.pullTillStop()

	// start handlers
	handlers := []*taskHandler{}
	for i := 0; i < opt.parallelism; i += 1 {
		handler := &taskHandler{
			id:             fmt.Sprintf("handler#%d", i),
			command:        opt.command,
			args:           opt.args,
			commandtimeout: opt.commandtimeout,
			retrytimeout:   opt.retrytimeout,
			ctx:            appCtx,
			msgCh:          msgCh,
			tokenCh:        tokenCh,
			doneCh:         make(chan bool, 1),
			tasklogname:    fmt.Sprintf("%s/task%d.log", opt.tasklogdir, i),
			maxtasklogkb:   opt.maxtasklogkb,
		}
		handlers = append(handlers, handler)
		go handler.handleTasks()
	}

	// await shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	signal.Stop(sigChan)
	log.Printf("start graceful shutdown")
	cancelApp()
	for _, handler := range handlers {
		<-handler.doneCh
	}
}
