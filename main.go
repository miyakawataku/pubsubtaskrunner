package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"log"
	"os"
	"os/exec"
)

type NewClientFun func(context.Context) (*pubsub.Client, error)

func MakeNewClientFun(opt opt) NewClientFun {
	return func(ctx context.Context) (*pubsub.Client, error) {
		clientOpts := option.WithServiceAccountFile(opt.credentials)
		return pubsub.NewClient(ctx, opt.project, clientOpts)
	}
}

func pullTillStop(newClient NewClientFun, subname string, ctx context.Context, msgCh chan<- *pubsub.Message, doneCh chan bool) {
	psClient, err := newClient(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	subs := psClient.Subscription(subname)
	it, err := subs.Pull(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	func() {
		defer it.Stop()
		for {
			msg, err := it.Next()
			if err != nil {
				log.Printf("stop iteration by %T: %v\n", err, err)
				break
			}
			msgCh <- msg
		}
	}()
}

func runTask(command string, args []string, ctx context.Context, msgCh <-chan *pubsub.Message) {
	for {
		select {
		case msg := <-msgCh:
			cmd := exec.Command(command, args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Stdin = bytes.NewReader(msg.Data)
			cmd.Start()
			cmd.Wait()
			msg.Done(true)
		}
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix("pubsubtaskrunner: ")
	opt := parseOpt()
	msgCh := make(chan *pubsub.Message)
	doneCh := make(chan bool)
	newClient := MakeNewClientFun(opt)
	for i := 0; i < opt.parallelism; i += 1 {
		go runTask(opt.command, opt.args, context.Background(), msgCh)
	}
	go pullTillStop(newClient, opt.subscription, context.Background(), msgCh, doneCh)
	<-doneCh
}
