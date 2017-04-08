package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

type e2eTestConf struct {
	project string
	topic   string
	subs    string
	cred    string
}

func getE2eTestConf() (*e2eTestConf, error) {
	var err error
	readEnv := func(envName string, p *string) {
		*p = os.Getenv(envName)
		if *p == "" {
			err = errors.New(fmt.Sprintf("environment variable %s not provided", envName))
		}
	}
	testConf := e2eTestConf{}
	readEnv("PUBSUBTASKRUNNER_TESTS_PROJECT", &testConf.project)
	readEnv("PUBSUBTASKRUNNER_TESTS_TOPIC", &testConf.topic)
	readEnv("PUBSUBTASKRUNNER_TESTS_SUBS", &testConf.subs)
	readEnv("PUBSUBTASKRUNNER_TESTS_CREDENTIALS", &testConf.cred)
	if err != nil {
		return nil, err
	} else {
		return &testConf, nil
	}
}

func TestEnd2End(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)

	testConf, err := getE2eTestConf()
	if err != nil {
		t.Error(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	psClient, err := pubsub.NewClient(ctx, testConf.project, option.WithServiceAccountFile(testConf.cred))
	if err != nil {
		t.Errorf("could not connect to Pub/Sub: %v", err)
		return
	}
	topic := psClient.Topic(testConf.topic)
	topic.Publish(ctx, &pubsub.Message{Data: []byte("foobar\n")})
	topic.Publish(ctx, &pubsub.Message{Data: []byte("barfoo\n")})

	conf := conf{
		command:        "/bin/sed",
		args:           []string{"s/foo/FOO/"},
		project:        testConf.project,
		subscription:   testConf.subs,
		credentials:    testConf.cred,
		parallelism:    2,
		tasklogdir:     tempDir,
		maxtasklogkb:   1000,
		taskttl:        time.Minute,
		commandtimeout: time.Second,
		termtimeout:    time.Second,
	}
	doMain(conf, func() { time.Sleep(time.Second * 20) })

	file1, err := ioutil.ReadFile(tempDir + "/task0.log")
	file2, err := ioutil.ReadFile(tempDir + "/task1.log")
	output := string(append(file1, file2...))
	expected1 := "FOObar\nbarFOO\n"
	expected2 := "barFOO\nFOObar\n"
	if output != expected1 && output != expected2 {
		t.Errorf("output of tasks must be %v or %v, but was %v", expected1, expected2, output)
	}
}
