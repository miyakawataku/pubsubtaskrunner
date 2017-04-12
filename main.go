package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// newPsClient returns a Pub/Sub client.
func newPsClient(ctx context.Context, conf conf) (*pubsub.Client, error) {
	var clientOpts []option.ClientOption
	if conf.credentials != "" {
		clientOpts = []option.ClientOption{option.WithServiceAccountFile(conf.credentials)}
	}
	return pubsub.NewClient(ctx, conf.project, clientOpts...)
}

// awaitSignal blocks till SIGINT or SIGTERM is sent.
func awaitSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	signal.Stop(sigChan)
}

// doMain does main job after reading configuration.
func doMain(conf conf, awaitSignal func(), logger *log.Logger) {
	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	respCh := make(chan *pubsub.Message)
	reqCh := make(chan struct{}, conf.parallelism)

	// start puller
	psClient, err := newPsClient(appCtx, conf)
	if err != nil {
		logger.Fatalf("could not make Pub/Sub client: %v", err)
	}
	puller := makePuller(taskPullerConf{
		subs:         psClient.Subscription(conf.subscription),
		maxExtension: conf.commandtimeout * time.Second * 5,
		respCh:       respCh,
		reqCh:        reqCh,
		logger:       logger,
	})
	go puller.pullTillShutdown(appCtx)

	// start handlers
	wg := &sync.WaitGroup{}
	wg.Add(conf.parallelism)
	for i := 0; i < conf.parallelism; i++ {
		handler := makeHandler(taskHandlerConf{
			id:             fmt.Sprintf("handler#%d", i),
			command:        conf.command,
			args:           conf.args,
			commandtimeout: conf.commandtimeout,
			termtimeout:    conf.termtimeout,
			taskttl:        conf.taskttl,
			respCh:         respCh,
			reqCh:          reqCh,
			wg:             wg,
			tasklogpath:    fmt.Sprintf("%s/task%d.log", conf.tasklogdir, i),
			maxtasklogkb:   conf.maxtasklogkb,
			logger:         logger,
		})
		go handler.handleTillShutdown(appCtx)
	}

	awaitSignal()

	logger.Print("start graceful shutdown")
	cancelApp()
	wg.Wait()
	logger.Print("shutdown succeeded")
}

func main() {
	logPrefix := fmt.Sprintf("pubsubtaskrunner: pid=%d: ", os.Getpid())
	logFlag := log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
	logger := log.New(os.Stderr, logPrefix, logFlag)
	conf := readConf()
	doMain(conf, awaitSignal, logger)
}
