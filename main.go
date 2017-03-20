package main

import (
	"cloud.google.com/go/pubsub"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// newPsClient returns a Pub/Sub client.
func newPsClient(opt opt, ctx context.Context) (*pubsub.Client, error) {
	clientOpts := option.WithServiceAccountFile(opt.credentials)
	return pubsub.NewClient(ctx, opt.project, clientOpts)
}

// awaitSignal blocks till SIGINT or SIGTERM is sent.
func awaitSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	signal.Stop(sigChan)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(fmt.Sprintf("pubsubtaskrunner: pid=%d: ", os.Getpid()))
	opt := parseOpt()

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	respCh := make(chan *pubsub.Message)
	reqCh := make(chan bool, opt.parallelism)

	// start puller
	psClient, err := newPsClient(opt, appCtx)
	if err != nil {
		log.Fatalf("could not make Pub/Sub client: %v", err)
	}
	puller := &taskPuller{
		subs:         psClient.Subscription(opt.subscription),
		maxExtension: opt.commandtimeout * time.Second * 5,
		respCh:       respCh,
		reqCh:        reqCh,
		initMsgIter:  initMsgIter,
		fetchMsg:     fetchMsg,
	}
	go puller.pullTillShutdown(appCtx)

	// start handlers
	doneChs := []<-chan bool{}
	for i := 0; i < opt.parallelism; i += 1 {
		doneCh := make(chan bool, 1)
		doneChs = append(doneChs, doneCh)
		handler := makeHandlerWithDefault(taskHandler{
			id:             fmt.Sprintf("handler#%d", i),
			command:        opt.command,
			args:           opt.args,
			commandtimeout: opt.commandtimeout,
			termtimeout:    opt.termtimeout,
			taskttl:        opt.taskttl,
			respCh:         respCh,
			reqCh:          reqCh,
			doneCh:         doneCh,
			tasklogpath:    fmt.Sprintf("%s/task%d.log", opt.tasklogdir, i),
			maxtasklogkb:   opt.maxtasklogkb,
		})
		go handler.handleTillShutdown(appCtx)
	}

	awaitSignal()

	log.Print("start graceful shutdown")
	cancelApp()
	for _, doneCh := range doneChs {
		<-doneCh
	}
	log.Print("shutdown succeeded")
}
