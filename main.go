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
	log.Printf("%s: start", handler.id)
	for {
		handler.tokenCh <- true
		select {
		case msg := <-handler.msgCh:
			handler.handleSingleTask(msg)
		case <-handler.ctx.Done():
			log.Printf("%s: shutdown", handler.id)
			handler.doneCh <- true
			return
		}
	}
}

func (handler *taskHandler) rotateLogIfNecessary() {
	stat, err := os.Stat(handler.tasklogname)
	switch {
	case os.IsNotExist(err):
		log.Printf("%s: create new task log %s", handler.id, handler.tasklogname)
		return
	case err != nil:
		log.Printf("%s: cannot stat %s: %v", handler.id, handler.tasklogname, err)
		return
	}

	if stat.Size() >= int64(handler.maxtasklogkb*1000) {
		prevLogName := handler.tasklogname + ".prev"
		log.Printf("%s: mv task log %s to %s", handler.id, handler.tasklogname, prevLogName)
		if err := os.Rename(handler.tasklogname, prevLogName); err != nil {
			log.Printf("%s: could not mv task log %s to %s: %v",
				handler.id, handler.tasklogname, prevLogName, err)
		}
	}
}

func (handler *taskHandler) handleSingleTask(msg *pubsub.Message) {
	retryDeadline := msg.PublishTime.Add(handler.retrytimeout)
	now := time.Now()
	if now.After(retryDeadline) {
		log.Printf("%s: message=%s: ack because of exceeded retry deadline %v",
			handler.id, msg.ID, retryDeadline)
		msg.Done(true)
		return
	}

	handler.rotateLogIfNecessary()
	tlfile, err := os.OpenFile(handler.tasklogname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("%s: message=%s: could not open task log %s: %v",
			handler.id, msg.ID, handler.tasklogname, err)
		msg.Done(false)
		return
	}
	defer tlfile.Close()

	log.Printf("%s: message=%s: process message", handler.id, msg.ID)
	cmd := exec.Command(handler.command, handler.args...)
	cmd.Stdout = tlfile
	cmd.Stderr = tlfile
	cmd.Stdin = bytes.NewReader(msg.Data)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		log.Printf("%s: message=%s: could not spawn command: %v", handler.id, msg.ID, err)
		msg.Done(false)
		return
	}

	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()
	select {
	case err := <-cmdDone:
		if err != nil {
			msg.Done(false)
			log.Printf("%s: message=%s: command failed: %v", handler.id, msg.ID, err)
		} else {
			msg.Done(true)
			log.Printf("%s: message=%s: command done", handler.id, msg.ID)
		}
	case <-time.After(handler.commandtimeout):
		log.Printf("%s: message=%s: kill process as command timeout reached", handler.id, msg.ID)
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-cmdDone:
			log.Printf("%s: message=%s: killed process", handler.id, msg.ID)
		case <-time.After(time.Second * 5):
			log.Printf("%s: message=%s: kill lingering process by SIGKILL", handler.id, msg.ID)
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
}

func newPsClient(opt opt, ctx context.Context) (*pubsub.Client, error) {
	clientOpts := option.WithServiceAccountFile(opt.credentials)
	return pubsub.NewClient(ctx, opt.project, clientOpts)
}

func awaitShutdown(cancelApp context.CancelFunc, doneChs []<-chan bool) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	signal.Stop(sigChan)
	log.Print("start graceful shutdown")
	cancelApp()
	for _, doneCh := range doneChs {
		<-doneCh
	}
	log.Print("shutdown succeeded")
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
	doneChs := []<-chan bool{}
	for i := 0; i < opt.parallelism; i += 1 {
		doneCh := make(chan bool, 1)
		doneChs = append(doneChs, doneCh)
		handler := &taskHandler{
			id:             fmt.Sprintf("handler#%d", i),
			command:        opt.command,
			args:           opt.args,
			commandtimeout: opt.commandtimeout,
			retrytimeout:   opt.retrytimeout,
			ctx:            appCtx,
			msgCh:          msgCh,
			tokenCh:        tokenCh,
			doneCh:         doneCh,
			tasklogname:    fmt.Sprintf("%s/task%d.log", opt.tasklogdir, i),
			maxtasklogkb:   opt.maxtasklogkb,
		}
		go handler.handleTasks()
	}

	awaitShutdown(cancelApp, doneChs)
}
