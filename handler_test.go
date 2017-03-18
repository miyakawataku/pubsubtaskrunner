package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// test rotateTaskLog

func TestRotateTaskLogRotateLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	tasklogname := tempDir + "/task.log"
	content := bytes.Repeat([]byte{0x5c}, 2001)
	ioutil.WriteFile(tasklogname, content, 0644)

	handler := &taskHandler{
		id:           "handler#001",
		tasklogname:  tasklogname,
		maxtasklogkb: 2,
	}
	isRotated, err := rotateTaskLog(handler)
	if !isRotated {
		t.Error("task log must be rotated, but not")
	}
	if err != nil {
		t.Error("must not cause error, but: %v", err)
	}

	prevtasklogname := tasklogname + ".prev"
	prevContent, err := ioutil.ReadFile(prevtasklogname)
	if err != nil {
		t.Errorf("task log not rotated to %s", prevtasklogname)
	}

	if bytes.Compare(prevContent, content) != 0 {
		t.Errorf("not expected content in prev task log: %v", prevContent)
	}

	stat, err := os.Stat(tasklogname)
	if stat != nil || !os.IsNotExist(err) {
		t.Errorf("old task log still exists: err=%v", err)
	}
}

func TestRotateTaskLogDoNotRotateLogDueToSize(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	tasklogname := tempDir + "/task.log"
	content := bytes.Repeat([]byte{0x5c}, 2000)
	ioutil.WriteFile(tasklogname, content, 0644)

	handler := &taskHandler{
		id:           "handler#001",
		tasklogname:  tasklogname,
		maxtasklogkb: 2,
	}

	isRotated, err := rotateTaskLog(handler)

	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err != nil {
		t.Error("must not cause error, but: %v", err)
	}

	prevtasklogname := tasklogname + ".prev"
	if _, err := os.Stat(prevtasklogname); !os.IsNotExist(err) {
		t.Errorf("prev log file must not exist, but: %v", err)
	}

	actualContent, err := ioutil.ReadFile(tasklogname)
	if err != nil {
		t.Errorf("cannot read task log %s: %v", tasklogname)
	}

	if bytes.Compare(actualContent, content) != 0 {
		t.Errorf("not expected content in task log: %v", actualContent)
	}
}

func TestRotateTaskLogDoNotRotateNonExistingLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tasklogname := tempDir + "/nosuch.log"
	handler := &taskHandler{
		id:           "handler#001",
		tasklogname:  tasklogname,
		maxtasklogkb: 2,
	}

	isRotated, err := rotateTaskLog(handler)
	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err != nil {
		t.Error("must not cause error, but: %v", err)
	}

	prevtasklogname := tasklogname + ".prev"
	if _, err := os.Stat(prevtasklogname); !os.IsNotExist(err) {
		t.Errorf("prev log file must not exist, but: %v", err)
	}
}

func TestRotateTaskLogDoNotRotateUnstattableLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	logDir := tempDir + "/tasklog.d"
	os.Mkdir(logDir, 0000)

	tasklogname := logDir + "/unstattable.log"
	handler := &taskHandler{
		id:           "handler#001",
		tasklogname:  tasklogname,
		maxtasklogkb: 2,
	}
	isRotated, err := rotateTaskLog(handler)
	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err == nil {
		t.Error("must cause error, but not")
	}
}

func TestRotateTaskLogDoNotRotateUnmovableLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	logDir := tempDir + "/log"
	os.Mkdir(logDir, 0700)
	tasklogname := logDir + "/task.log"
	content := bytes.Repeat([]byte{0x5c}, 2001)
	ioutil.WriteFile(tasklogname, content, 0644)
	os.Chmod(logDir, 0500)
	defer os.Chmod(logDir, 0700)

	handler := &taskHandler{
		id:           "handler#001",
		tasklogname:  tasklogname,
		maxtasklogkb: 2,
	}
	isRotated, err := rotateTaskLog(handler)
	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err == nil {
		t.Error("must cause error, but not")
	}

	prevtasklogname := tasklogname + ".prev"
	if _, err := os.Stat(prevtasklogname); !os.IsNotExist(err) {
		t.Errorf("prev log file must not exist, but: %v", err)
	}

	actualContent, err := ioutil.ReadFile(tasklogname)
	if err != nil {
		t.Errorf("cannot read task log %s: %v", tasklogname)
	}

	if bytes.Compare(actualContent, content) != 0 {
		t.Errorf("not expected content in task log: %v", actualContent)
	}
}

// test runCmd

func TestRunCmd(t *testing.T) {
	handler := &taskHandler{
		id:             "handler#001",
		command:        "/bin/cat",
		args:           []string{"-"},
		commandtimeout: time.Second * 10,
		termtimeout:    time.Second,
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
		termtimeout:    time.Second,
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
		termtimeout:    time.Second,
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
		termtimeout:    time.Second,
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
