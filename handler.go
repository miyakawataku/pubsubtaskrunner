package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
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
	id             string
	command        string
	args           []string
	commandtimeout time.Duration
	retrytimeout   time.Duration
	respCh         <-chan *pubsub.Message
	reqCh          chan<- bool
	doneCh         chan bool
	tasklogname    string
	maxtasklogkb   int
	ack            ackNackFunc
	nack           ackNackFunc
	now            nowFunc
	openTaskLog    openTaskLogFunc
	rotateTaskLog  rotateTaskLogFunc
}

type ackNackFunc func(msg *pubsub.Message)
type nowFunc func() time.Time

func makeHandlerWithDefault(handler taskHandler) *taskHandler {
	handler.ack = func(msg *pubsub.Message) { msg.Done(true) }
	handler.nack = func(msg *pubsub.Message) { msg.Done(false) }
	handler.now = time.Now
	handler.openTaskLog = openTaskLog
	handler.rotateTaskLog = rotateTaskLog
	return &handler
}

func (handler *taskHandler) handleTasks(ctx context.Context) {
	log.Printf("%s: start", handler.id)
	for {
		handler.reqCh <- true
		select {
		case msg := <-handler.respCh:
			handler.handleSingleTask(msg)
		case <-ctx.Done():
			log.Printf("%s: shutdown", handler.id)
			handler.doneCh <- true
			return
		}
	}
}

func (handler *taskHandler) handleSingleTask(msg *pubsub.Message) {
	retryDeadline := msg.PublishTime.Add(handler.retrytimeout)
	if handler.now().After(retryDeadline) {
		log.Printf("%s: message=%s: delete task because of exceeded retry deadline %v",
			handler.id, msg.ID, retryDeadline)
		handler.ack(msg)
		return
	}

	handler.rotateTaskLog(handler)
	taskLog, err := handler.openTaskLog(handler)
	if err != nil {
		log.Printf("%s: message=%s: could not open task log %s: %v",
			handler.id, msg.ID, handler.tasklogname, err)
		handler.nack(msg)
		return
	}
	defer taskLog.Close()

	if err := runCmd(handler, msg, taskLog); err != nil {
		log.Printf("%s: message=%s: command failed: %v", handler.id, msg.ID, err)
		handler.nack(msg)
	} else {
		log.Printf("%s: message=%s: command done", handler.id, msg.ID)
		handler.ack(msg)
	}
}

type rotateTaskLogFunc func(handler *taskHandler)

func rotateTaskLog(handler *taskHandler) {
	stat, err := os.Stat(handler.tasklogname)
	switch {
	case os.IsNotExist(err):
		log.Printf("%s: create new task log %s", handler.id, handler.tasklogname)
		return
	case err != nil:
		log.Printf("%s: cannot stat %s: %v", handler.id, handler.tasklogname, err)
		return
	}

	if stat.Size() >= int64(handler.maxtasklogkb*1000) {
		prevLogName := handler.tasklogname + ".prev"
		log.Printf("%s: mv task log %s to %s", handler.id, handler.tasklogname, prevLogName)
		if err := os.Rename(handler.tasklogname, prevLogName); err != nil {
			log.Printf("%s: could not mv task log %s to %s: %v",
				handler.id, handler.tasklogname, prevLogName, err)
		}
	}
}

type openTaskLogFunc func(handler *taskHandler) (io.WriteCloser, error)

func openTaskLog(handler *taskHandler) (io.WriteCloser, error) {
	return os.OpenFile(handler.tasklogname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
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
		termTimeout := time.Second * 5
		select {
		case <-cmdDone:
			return cmdTimeoutError(pgid)
		case <-time.After(termTimeout):
			log.Printf("%s: message=%s: command-pgid=%d: command didn't terminate in %v; will SIGKILL process group",
				handler.id, msg.ID, pgid, termTimeout)
			syscall.Kill(-pgid, syscall.SIGKILL)
			return cmdTermTimeoutError(pgid)
		}
	}
}
