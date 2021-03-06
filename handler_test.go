package main

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

// test handleTillShutdown

func makeHandleSingleTaskFunc(callCount *int, actions []handleSingleTaskFunc) handleSingleTaskFunc {
	return func(handler *taskHandler, msg *pubsub.Message) msgNotifier {
		index := *callCount
		*callCount++
		if index >= len(actions) {
			return nil
		}
		action := actions[index]
		return action(handler, msg)
	}
}

type fakeMsgNotifier struct {
	t          *testing.T
	desc       string
	isNotified bool
}

func (notifier *fakeMsgNotifier) notify(handler *taskHandler, msg *pubsub.Message) {
	notifier.t.Logf("notified %s", notifier.desc)
	notifier.isNotified = true
}

func TestHandleTillShutdown(t *testing.T) {
	reqCh := make(chan struct{}, 4)
	respCh := make(chan *pubsub.Message, 3)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	callCount := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	not1 := &fakeMsgNotifier{t: t, desc: "not1"}
	not2 := &fakeMsgNotifier{t: t, desc: "not2"}
	not3 := &fakeMsgNotifier{t: t, desc: "not3"}
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:     "handler#001",
			reqCh:  reqCh,
			respCh: respCh,
			wg:     wg,
			logger: makeTestLogger(t),
		},
		handleSingleTask: makeHandleSingleTaskFunc(&callCount, []handleSingleTaskFunc{
			func(handler *taskHandler, msg *pubsub.Message) msgNotifier { return not1 },
			func(handler *taskHandler, msg *pubsub.Message) msgNotifier { return not2 },
			func(handler *taskHandler, msg *pubsub.Message) msgNotifier { cancel(); return not3 },
		}),
	}
	respCh <- nil
	respCh <- nil
	respCh <- nil
	go handler.handleTillShutdown(ctx)
	wg.Wait()
	if callCount != 3 {
		t.Errorf("handleSingleTask must be called 3 times, but called %d times", callCount)
	}
	if !not1.isNotified || !not2.isNotified || !not3.isNotified {
		t.Errorf("some notifier not invoked: %v, %v, %v", not1, not2, not3)
	}
}

// test handleSingleTask

func TestHandleSingleTaskAckForExceedRetryDeadline(t *testing.T) {
	pubTime := time.Date(2017, 3, 1, 0, 0, 0, 0, time.UTC)
	msg := &pubsub.Message{
		PublishTime: pubTime,
		ID:          "msg001",
	}
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:      "handler#001",
			taskttl: time.Minute * 10,
			logger:  makeTestLogger(t),
		},
		now: func() time.Time {
			return pubTime.Add(time.Minute*10 + time.Second)
		},
	}
	notifier := handleSingleTask(handler, msg)
	if notifier != ack {
		t.Errorf("handleSingleTask must return ack because of exceeded deadline, but returned %v", notifier)
	}
}

func TestHandleSingleTaskNackForLogRotationFailure(t *testing.T) {
	pubTime := time.Date(2017, 3, 1, 0, 0, 0, 0, time.UTC)
	msg := &pubsub.Message{
		PublishTime: pubTime,
		ID:          "msg001",
	}
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:      "handler#001",
			taskttl: time.Minute * 10,
			logger:  makeTestLogger(t),
		},
		now: func() time.Time {
			return pubTime.Add(time.Minute * 10)
		},
		rotateTaskLog: func(handler *taskHandler) (bool, error) {
			return false, errors.New("bang")
		},
	}
	notifier := handleSingleTask(handler, msg)
	if notifier != nack {
		t.Errorf("handleSingleTask must return nack because of log rotation failure, but returned %v", notifier)
	}
}

func TestHandleSingleTaskNackForLogOpeningFailure(t *testing.T) {
	pubTime := time.Date(2017, 3, 1, 0, 0, 0, 0, time.UTC)
	msg := &pubsub.Message{
		PublishTime: pubTime,
		ID:          "msg001",
	}
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:      "handler#001",
			taskttl: time.Minute * 10,
			logger:  makeTestLogger(t),
		},
		now: func() time.Time {
			return pubTime.Add(time.Minute * 10)
		},
		rotateTaskLog: func(handler *taskHandler) (bool, error) {
			return true, nil
		},
		openTaskLog: func(handler *taskHandler) (io.WriteCloser, error) {
			return nil, errors.New("bang")
		},
	}
	notifier := handleSingleTask(handler, msg)
	if notifier != nack {
		t.Errorf("handleSingleTask must return nack because of log opening failure, but returned %v", notifier)
	}
}

type fakeWriteCloser struct {
	isClosed bool
}

func (fwc *fakeWriteCloser) Write(p []byte) (int, error) {
	return 0, nil
}

func (fwc *fakeWriteCloser) Close() error {
	fwc.isClosed = true
	return nil
}

func TestHandleSingleTaskAckForCommandSuccess(t *testing.T) {
	pubTime := time.Date(2017, 3, 1, 0, 0, 0, 0, time.UTC)
	msg := &pubsub.Message{
		PublishTime: pubTime,
		ID:          "msg001",
	}
	fwc := &fakeWriteCloser{}
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:      "handler#001",
			taskttl: time.Minute * 10,
			logger:  makeTestLogger(t),
		},
		now: func() time.Time {
			return pubTime.Add(time.Minute * 10)
		},
		rotateTaskLog: func(handler *taskHandler) (bool, error) {
			return true, nil
		},
		openTaskLog: func(handler *taskHandler) (io.WriteCloser, error) {
			return fwc, nil
		},
		runCmd: func(handler *taskHandler, msg *pubsub.Message, taskLog io.Writer) error {
			return nil
		},
	}
	notifier := handleSingleTask(handler, msg)
	if notifier != ack {
		t.Errorf("handleSingleTask must return ack because of log command success, but returned %v", notifier)
	}
}

func TestHandleSingleTaskAckForCommandFailure(t *testing.T) {
	pubTime := time.Date(2017, 3, 1, 0, 0, 0, 0, time.UTC)
	msg := &pubsub.Message{
		PublishTime: pubTime,
		ID:          "msg001",
	}
	fwc := &fakeWriteCloser{}
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:      "handler#001",
			taskttl: time.Minute * 10,
			logger:  makeTestLogger(t),
		},
		now: func() time.Time {
			return pubTime.Add(time.Minute * 10)
		},
		rotateTaskLog: func(handler *taskHandler) (bool, error) {
			return true, nil
		},
		openTaskLog: func(handler *taskHandler) (io.WriteCloser, error) {
			return fwc, nil
		},
		runCmd: func(handler *taskHandler, msg *pubsub.Message, taskLog io.Writer) error {
			return errors.New("bang")
		},
	}
	notifier := handleSingleTask(handler, msg)
	if notifier != nack {
		t.Errorf("handleSingleTask must return nack because of log command failure, but returned %v", notifier)
	}

}

// test rotateTaskLog

func TestRotateTaskLogRotateLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)
	tasklogpath := tempDir + "/task.log"
	content := bytes.Repeat([]byte{0x5c}, 2001)
	ioutil.WriteFile(tasklogpath, content, 0644)

	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:           "handler#001",
			tasklogpath:  tasklogpath,
			maxtasklogkb: 2,
			logger:       makeTestLogger(t),
		},
	}
	isRotated, err := rotateTaskLog(handler)
	if !isRotated {
		t.Error("task log must be rotated, but not")
	}
	if err != nil {
		t.Errorf("must not cause error, but: %v", err)
	}

	prevlogpath := tasklogpath + ".prev"
	prevContent, err := ioutil.ReadFile(prevlogpath)
	if err != nil {
		t.Errorf("task log not rotated to %s", prevlogpath)
	}

	if !bytes.Equal(prevContent, content) {
		t.Errorf("not expected content in prev task log: %v", prevContent)
	}

	stat, err := os.Stat(tasklogpath)
	if stat != nil || !os.IsNotExist(err) {
		t.Errorf("old task log still exists: err=%v", err)
	}
}

func TestRotateTaskLogDoNotRotateLogDueToSize(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)
	tasklogpath := tempDir + "/task.log"
	content := bytes.Repeat([]byte{0x5c}, 2000)
	ioutil.WriteFile(tasklogpath, content, 0644)

	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:           "handler#001",
			tasklogpath:  tasklogpath,
			maxtasklogkb: 2,
			logger:       makeTestLogger(t),
		},
	}

	isRotated, err := rotateTaskLog(handler)

	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err != nil {
		t.Errorf("must not cause error, but: %v", err)
	}

	prevlogpath := tasklogpath + ".prev"
	if _, err := os.Stat(prevlogpath); !os.IsNotExist(err) {
		t.Errorf("prev log file must not exist, but: %v", err)
	}

	actualContent, err := ioutil.ReadFile(tasklogpath)
	if err != nil {
		t.Errorf("cannot read task log %s: %v", tasklogpath, err)
	}

	if !bytes.Equal(actualContent, content) {
		t.Errorf("not expected content in task log: %v", actualContent)
	}
}

func TestRotateTaskLogDoNotRotateNonExistingLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)

	tasklogpath := tempDir + "/nosuch.log"
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:           "handler#001",
			tasklogpath:  tasklogpath,
			maxtasklogkb: 2,
			logger:       makeTestLogger(t),
		},
	}

	isRotated, err := rotateTaskLog(handler)
	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err != nil {
		t.Errorf("must not cause error, but: %v", err)
	}

	prevlogpath := tasklogpath + ".prev"
	if _, err := os.Stat(prevlogpath); !os.IsNotExist(err) {
		t.Errorf("prev log file must not exist, but: %v", err)
	}
}

func TestRotateTaskLogDoNotRotateUnstattableLog(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "pubsubtaskrunnertest")
	if err != nil {
		t.Errorf("could not create a temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)
	logDir := tempDir + "/tasklog.d"
	os.Mkdir(logDir, 0000)

	tasklogpath := logDir + "/unstattable.log"
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:           "handler#001",
			tasklogpath:  tasklogpath,
			maxtasklogkb: 2,
			logger:       makeTestLogger(t),
		},
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
		return
	}
	defer os.RemoveAll(tempDir)
	logDir := tempDir + "/log"
	os.Mkdir(logDir, 0700)
	tasklogpath := logDir + "/task.log"
	content := bytes.Repeat([]byte{0x5c}, 2001)
	ioutil.WriteFile(tasklogpath, content, 0644)
	os.Chmod(logDir, 0500)
	defer os.Chmod(logDir, 0700)

	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:           "handler#001",
			tasklogpath:  tasklogpath,
			maxtasklogkb: 2,
			logger:       makeTestLogger(t),
		},
	}
	isRotated, err := rotateTaskLog(handler)
	if isRotated {
		t.Error("task log must not be rotated, but was")
	}
	if err == nil {
		t.Error("must cause error, but not")
	}

	prevlogpath := tasklogpath + ".prev"
	if _, err := os.Stat(prevlogpath); !os.IsNotExist(err) {
		t.Errorf("prev log file must not exist, but: %v", err)
	}

	actualContent, err := ioutil.ReadFile(tasklogpath)
	if err != nil {
		t.Errorf("cannot read task log %s: %v", tasklogpath, err)
	}

	if !bytes.Equal(actualContent, content) {
		t.Errorf("not expected content in task log: %v", actualContent)
	}
}

// test runCmd

func TestRunCmd(t *testing.T) {
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:             "handler#001",
			command:        "/bin/cat",
			args:           []string{"-"},
			commandtimeout: time.Second * 10,
			termtimeout:    time.Second,
			logger:         makeTestLogger(t),
		},
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
	if !bytes.Equal(buf.Bytes(), []byte("foobar")) {
		t.Errorf("unexpected output: got %v, want 'foobar'", buf.Bytes())
	}
}

func TestRunCmdTimeout(t *testing.T) {
	handler := &taskHandler{
		taskHandlerConf: taskHandlerConf{
			id:             "handler#001",
			command:        "/bin/sleep",
			args:           []string{"5"},
			commandtimeout: time.Second,
			termtimeout:    time.Second,
			logger:         makeTestLogger(t),
		},
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
		taskHandlerConf: taskHandlerConf{
			id:             "handler#001",
			command:        "/bin/sh",
			args:           []string{"-c", "trap '/bin/true' 15; while /bin/true; do /bin/sleep 100; done"},
			commandtimeout: time.Second,
			termtimeout:    time.Second,
			logger:         makeTestLogger(t),
		},
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
		taskHandlerConf: taskHandlerConf{
			id:             "handler#001",
			command:        "/bin/no/such/command.never",
			args:           []string{},
			commandtimeout: time.Second,
			termtimeout:    time.Second,
			logger:         makeTestLogger(t),
		},
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
