package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"testing"
	"time"
)

// test runCmd

func TestRunCmd(t *testing.T) {
	handler := &taskHandler{
		id:             "handler#001",
		command:        "/bin/cat",
		args:           []string{"-"},
		commandtimeout: time.Second * 10,
	}
	msg := &pubsub.Message{
		ID:   "msg#001",
		Data: []byte("foobar"),
	}
	buf := bytes.NewBuffer([]byte{})
	err := runCmd(handler, msg, buf)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if bytes.Compare(buf.Bytes(), []byte("foobar")) != 0 {
		t.Errorf("unexpected output: got %v, want 'foobar'", buf.Bytes())
	}
}

func TestRunCmdTimeout(t *testing.T) {
	handler := &taskHandler{
		id:             "handler#001",
		command:        "/bin/sleep",
		args:           []string{"5"},
		commandtimeout: time.Second,
	}
	msg := &pubsub.Message{
		ID:   "msg#001",
		Data: []byte{},
	}
	buf := bytes.NewBuffer([]byte{})
	err := runCmd(handler, msg, buf)
	if _, ok := err.(cmdTimeoutError); !ok {
		t.Errorf("unexpected err: got %v, want cmdTimeoutError", err)
	}
}

func TestRunCmdTermTimeout(t *testing.T) {
	handler := &taskHandler{
		id:             "handler#001",
		command:        "/bin/sh",
		args:           []string{"-c", "trap '/bin/true' 15; while /bin/true; do /bin/sleep 100; done"},
		commandtimeout: time.Second,
	}
	msg := &pubsub.Message{
		ID:   "msg#001",
		Data: []byte{},
	}
	buf := bytes.NewBuffer([]byte{})
	err := runCmd(handler, msg, buf)
	if _, ok := err.(cmdTermTimeoutError); !ok {
		t.Errorf("unexpected err: got %v, want cmdTermTimeoutError", err)
	}
}

func TestRunCmdLaunchError(t *testing.T) {
	handler := &taskHandler{
		id:             "handler#001",
		command:        "/bin/no/such/command.never",
		args:           []string{},
		commandtimeout: time.Second,
	}
	msg := &pubsub.Message{
		ID:   "msg#001",
		Data: []byte{},
	}
	buf := bytes.NewBuffer([]byte{})
	err := runCmd(handler, msg, buf)
	if _, ok := err.(spawnError); !ok {
		t.Errorf("unexpected err: got %v, want spawnError", err)
	}
}
