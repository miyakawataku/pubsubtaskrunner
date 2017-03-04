package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"log"
	"os"
	"os/exec"
	"time"
)

type NewClientFun func(context.Context) (*pubsub.Client, error)

func MakeNewClientFun(opt opt) NewClientFun {
	return func(ctx context.Context) (*pubsub.Client, error) {
		clientOpts := option.WithServiceAccountFile(opt.credentials)
		return pubsub.NewClient(ctx, opt.project, clientOpts)
	}
}

type taskPuller struct {
	newClient      NewClientFun
	subname        string
	commandtimeout time.Duration
	ctx            context.Context
	msgCh          chan<- *pubsub.Message
	tokenCh        <-chan bool
	doneCh         chan bool
}

func (puller *taskPuller) pullTillStop() {
	psClient, err := puller.newClient(puller.ctx)
	if err != nil {
		log.Fatalln(err)
	}
	subs := psClient.Subscription(puller.subname)
	it, err := subs.Pull(puller.ctx, pubsub.MaxExtension(puller.commandtimeout+time.Second*5))
	if err != nil {
		log.Fatalln(err)
	}

	func() {
		defer it.Stop()
		for {
			<-puller.tokenCh
			msg, err := it.Next()
			if err != nil {
				log.Printf("stop iteration by %T: %v\n", err, err)
				break
			}
			puller.msgCh <- msg
		}
	}()
}

type taskHandler struct {
	command        string
	args           []string
	commandtimeout time.Duration
	ctx            context.Context
	msgCh          <-chan *pubsub.Message
	tokenCh        chan<- bool
}

func (handler *taskHandler) handleTasks() {
	for {
		handler.tokenCh <- true
		select {
		case msg := <-handler.msgCh:
			cmd := exec.Command(handler.command, handler.args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Stdin = bytes.NewReader(msg.Data)
			cmd.Start()
			cmdDone := make(chan error, 1)
			go func() {
				cmdDone <- cmd.Wait()
			}()
			select {
			case <-time.After(handler.commandtimeout):
				if err := cmd.Process.Kill(); err != nil {
					log.Fatal("failed to kill: ", err)
				}
				log.Printf("process killed as timeout reached")
			case err := <-cmdDone:
				if err != nil {
					msg.Done(false)
					log.Printf("command failed: %v", err)
				} else {
					msg.Done(true)
					log.Print("command done")
				}
			}
		}
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix("pubsubtaskrunner: ")
	opt := parseOpt()
	msgCh := make(chan *pubsub.Message)
	doneCh := make(chan bool)
	tokenCh := make(chan bool, opt.parallelism)
	for i := 0; i < opt.parallelism; i += 1 {
		handler := &taskHandler{
			command:        opt.command,
			args:           opt.args,
			commandtimeout: opt.commandtimeout,
			ctx:            context.Background(),
			msgCh:          msgCh,
			tokenCh:        tokenCh,
		}
		go handler.handleTasks()
	}

	puller := &taskPuller{
		newClient:      MakeNewClientFun(opt),
		subname:        opt.subscription,
		commandtimeout: opt.commandtimeout,
		ctx:            context.Background(),
		msgCh:          msgCh,
		tokenCh:        tokenCh,
		doneCh:         doneCh,
	}
	go puller.pullTillStop()
	<-doneCh
}
