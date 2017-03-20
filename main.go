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
func newPsClient(conf conf, ctx context.Context) (*pubsub.Client, error) {
	if conf.credentials == "" {
		return pubsub.NewClient(ctx, conf.project)
	} else {
		clientOpts := option.WithServiceAccountFile(conf.credentials)
		return pubsub.NewClient(ctx, conf.project, clientOpts)
	}
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
	conf := readConf()

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	respCh := make(chan *pubsub.Message)
	reqCh := make(chan bool, conf.parallelism)

	// start puller
	psClient, err := newPsClient(conf, appCtx)
	if err != nil {
		log.Fatalf("could not make Pub/Sub client: %v", err)
	}
	puller := makePullerWithDefault(taskPuller{
		subs:         psClient.Subscription(conf.subscription),
		maxExtension: conf.commandtimeout * time.Second * 5,
		respCh:       respCh,
		reqCh:        reqCh,
	})
	go puller.pullTillShutdown(appCtx)

	// start handlers
	doneChs := []<-chan bool{}
	for i := 0; i < conf.parallelism; i += 1 {
		doneCh := make(chan bool, 1)
		doneChs = append(doneChs, doneCh)
		handler := makeHandlerWithDefault(taskHandler{
			id:             fmt.Sprintf("handler#%d", i),
			command:        conf.command,
			args:           conf.args,
			commandtimeout: conf.commandtimeout,
			termtimeout:    conf.termtimeout,
			taskttl:        conf.taskttl,
			respCh:         respCh,
			reqCh:          reqCh,
			doneCh:         doneCh,
			tasklogpath:    fmt.Sprintf("%s/task%d.log", conf.tasklogdir, i),
			maxtasklogkb:   conf.maxtasklogkb,
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
