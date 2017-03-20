package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type taskHandler struct {
	id               string
	command          string
	args             []string
	commandtimeout   time.Duration
	termtimeout      time.Duration
	retrytimeout     time.Duration
	respCh           <-chan *pubsub.Message
	reqCh            chan<- bool
	doneCh           chan bool
	tasklogname      string
	maxtasklogkb     int
	now              nowFunc
	handleSingleTask handleSingleTaskFunc
	openTaskLog      openTaskLogFunc
	rotateTaskLog    rotateTaskLogFunc
	runCmd           runCmdFunc
}

type ackNackFunc func(msg *pubsub.Message)
type nowFunc func() time.Time

func makeHandlerWithDefault(handler taskHandler) *taskHandler {
	handler.now = time.Now
	handler.handleSingleTask = handleSingleTask
	handler.openTaskLog = openTaskLog
	handler.rotateTaskLog = rotateTaskLog
	handler.runCmd = runCmd
	return &handler
}

func (handler *taskHandler) handleTasks(ctx context.Context) {
	log.Printf("%s: start", handler.id)
	for {
		handler.reqCh <- true
		select {
		case msg := <-handler.respCh:
			notifier := handler.handleSingleTask(handler, msg)
			notifier.notify(handler, msg)
		case <-ctx.Done():
			log.Printf("%s: shutdown", handler.id)
			handler.doneCh <- true
			return
		}
	}
}

type resultNotifier interface {
	notify(handler *taskHandler, msg *pubsub.Message)
}

type fixedStatusResultNotifier struct {
	result bool
	desc   string
}

func (rn *fixedStatusResultNotifier) notify(handler *taskHandler, msg *pubsub.Message) {
	log.Printf("%s: message=%s: %s the message", handler.id, msg.ID, rn.desc)
	msg.Done(rn.result)
}

var (
	ack  resultNotifier
	nack resultNotifier
)

func init() {
	ack = &fixedStatusResultNotifier{
		result: true,
		desc:   "ack",
	}
	nack = &fixedStatusResultNotifier{
		result: false,
		desc:   "nack",
	}
}

type handleSingleTaskFunc func(handler *taskHandler, msg *pubsub.Message) resultNotifier

func handleSingleTask(handler *taskHandler, msg *pubsub.Message) resultNotifier {
	retryDeadline := msg.PublishTime.Add(handler.retrytimeout)
	if handler.now().After(retryDeadline) {
		log.Printf("%s: message=%s: delete task because of exceeded retry deadline %v",
			handler.id, msg.ID, retryDeadline)
		return ack
	}

	if _, err := handler.rotateTaskLog(handler); err != nil {
		log.Printf("%s: message=%s: could not rotate task log %s: %v",
			handler.id, msg.ID, handler.tasklogname, err)
		return nack
	}

	taskLog, err := handler.openTaskLog(handler)
	if err != nil {
		log.Printf("%s: message=%s: could not open task log %s: %v",
			handler.id, msg.ID, handler.tasklogname, err)
		return nack
	}
	defer taskLog.Close()

	if err := handler.runCmd(handler, msg, taskLog); err != nil {
		log.Printf("%s: message=%s: command failed: %v", handler.id, msg.ID, err)
		return nack
	} else {
		log.Printf("%s: message=%s: command done", handler.id, msg.ID)
		return ack
	}
}

type rotateTaskLogFunc func(handler *taskHandler) (bool, error)

func rotateTaskLog(handler *taskHandler) (bool, error) {
	stat, err := os.Stat(handler.tasklogname)
	switch {
	case os.IsNotExist(err):
		log.Printf("%s: create new task log %s", handler.id, handler.tasklogname)
		return false, nil
	case err != nil:
		return false, errors.New(fmt.Sprintf("cannot stat %s: %v", handler.tasklogname, err))
	}

	if stat.Size() > int64(handler.maxtasklogkb*1000) {
		prevLogName := handler.tasklogname + ".prev"
		log.Printf("%s: mv task log %s to %s", handler.id, handler.tasklogname, prevLogName)
		if err := os.Rename(handler.tasklogname, prevLogName); err != nil {
			return false, errors.New(fmt.Sprintf(
				"could not mv task log %s to %s: %v",
				handler.tasklogname, prevLogName, err))
		}
		return true, nil
	} else {
		return false, nil
	}
}

type openTaskLogFunc func(handler *taskHandler) (io.WriteCloser, error)

func openTaskLog(handler *taskHandler) (io.WriteCloser, error) {
	return os.OpenFile(handler.tasklogname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
}

type spawnError struct {
	cause error
}

func (err spawnError) Error() string {
	return fmt.Sprintf("could not spawn command: %v", err.cause)
}

type cmdTimeoutError int

func (pgid cmdTimeoutError) Error() string {
	return fmt.Sprintf("command timeout: command-pgid=%d", pgid)
}

type cmdTermTimeoutError int

func (pgid cmdTermTimeoutError) Error() string {
	return fmt.Sprintf("command termination timeout: command-pgid=%d", pgid)
}

type runCmdFunc func(handler *taskHandler, msg *pubsub.Message, taskLog io.Writer) error

func runCmd(handler *taskHandler, msg *pubsub.Message, taskLog io.Writer) error {
	cmd := exec.Command(handler.command, handler.args...)
	cmd.Stdout = taskLog
	cmd.Stderr = taskLog
	cmd.Stdin = bytes.NewReader(msg.Data)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return spawnError{err}
	}
	pgid := cmd.Process.Pid
	log.Printf("%s: message=%s: command-pgid=%d: spawned the command",
		handler.id, msg.ID, pgid)

	cmdDone := make(chan error, 1)
	go func() { cmdDone <- cmd.Wait() }()
	select {
	case err := <-cmdDone:
		return err
	case <-time.After(handler.commandtimeout):
		log.Printf("%s: message=%s: command-pgid=%d: command didn't complete in %v; will SIGTERM process group",
			handler.id, msg.ID, pgid, handler.commandtimeout)
		syscall.Kill(-pgid, syscall.SIGTERM)
		select {
		case <-cmdDone:
			return cmdTimeoutError(pgid)
		case <-time.After(handler.termtimeout):
			log.Printf("%s: message=%s: command-pgid=%d: command didn't terminate in %v; will SIGKILL process group",
				handler.id, msg.ID, pgid, handler.termtimeout)
			syscall.Kill(-pgid, syscall.SIGKILL)
			return cmdTermTimeoutError(pgid)
		}
	}
}
