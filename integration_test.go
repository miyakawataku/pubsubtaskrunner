// +build integration

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

func TestIntegration(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	psClient, err := pubsub.NewClient(ctx, *project, option.WithServiceAccountFile(*credentials))
	if err != nil {
		t.Errorf("could not connect to Pub/Sub: %v", err)
		return
	}
	defer psClient.Close()

	topic := psClient.Topic(*topic)
	topic.Publish(ctx, &pubsub.Message{Data: []byte("foobar\n")})
	topic.Publish(ctx, &pubsub.Message{Data: []byte("barfoo\n")})

	conf := conf{
		command:        "/bin/sed",
		args:           []string{"s/foo/FOO/"},
		project:        *project,
		subscription:   *subscription,
		credentials:    *credentials,
		parallelism:    2,
		tasklogdir:     tempDir,
		maxtasklogkb:   1000,
		taskttl:        time.Minute,
		commandtimeout: time.Second,
		termtimeout:    time.Second,
	}
	doMain(conf, func() { time.Sleep(time.Second * 20) }, makeTestLogger(t))

	file1, _ := ioutil.ReadFile(tempDir + "/task0.log")
	file2, _ := ioutil.ReadFile(tempDir + "/task1.log")
	output := string(append(file1, file2...))
	expected1 := "FOObar\nbarFOO\n"
	expected2 := "barFOO\nFOObar\n"
	if output != expected1 && output != expected2 {
		t.Errorf("output of tasks must be %v or %v, but was %v", expected1, expected2, output)
	}
}

var (
	project      = flag.String("project", "", "GCP project ID")
	topic        = flag.String("topic", "", "Pub/Sub topic ID")
	subscription = flag.String("subscription", "", "Pub/Sub subscription ID")
	credentials  = flag.String("credentials", "", "path to credentials JSON file")
)

func TestMain(m *testing.M) {
	flag.Parse()
	if *project == "" || *topic == "" || *subscription == "" {
		fmt.Fprintln(os.Stderr, "--project, --topic, --subscription flags are all required for integration test")
		os.Exit(1)
	}
	os.Exit(m.Run())
}
