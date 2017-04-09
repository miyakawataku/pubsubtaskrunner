package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
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

// doMain does main job after reading configuration.
func doMain(conf conf, awaitSignal func()) {
	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	respCh := make(chan *pubsub.Message)
	reqCh := make(chan struct{}, conf.parallelism)

	// start puller
	psClient, err := newPsClient(conf, appCtx)
	if err != nil {
		log.Fatalf("could not make Pub/Sub client: %v", err)
	}
	puller := makePuller(taskPullerConf{
		subs:         psClient.Subscription(conf.subscription),
		maxExtension: conf.commandtimeout * time.Second * 5,
		respCh:       respCh,
		reqCh:        reqCh,
	})
	go puller.pullTillShutdown(appCtx)

	// start handlers
	doneChs := []<-chan struct{}{}
	for i := 0; i < conf.parallelism; i += 1 {
		doneCh := make(chan struct{}, 1)
		doneChs = append(doneChs, doneCh)
		handler := makeHandler(taskHandlerConf{
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

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(fmt.Sprintf("pubsubtaskrunner: pid=%d: ", os.Getpid()))
	conf := readConf()
	doMain(conf, awaitSignal)
}
