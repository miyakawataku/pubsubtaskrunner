package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
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
	ctx            context.Context
	msgCh          <-chan *pubsub.Message
	tokenCh        chan<- bool
	doneCh         chan bool
	tasklogname    string
	maxtasklogkb   int
}

func (handler *taskHandler) handleTasks() {
	log.Printf("%s: start", handler.id)
	for {
		handler.tokenCh <- true
		select {
		case msg := <-handler.msgCh:
			handler.handleSingleTask(msg)
		case <-handler.ctx.Done():
			log.Printf("%s: shutdown", handler.id)
			handler.doneCh <- true
			return
		}
	}
}

func (handler *taskHandler) rotateLogIfNecessary() {
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

func (handler *taskHandler) handleSingleTask(msg *pubsub.Message) {
	retryDeadline := msg.PublishTime.Add(handler.retrytimeout)
	now := time.Now()
	if now.After(retryDeadline) {
		log.Printf("%s: message=%s: ack because of exceeded retry deadline %v",
			handler.id, msg.ID, retryDeadline)
		msg.Done(true)
		return
	}

	handler.rotateLogIfNecessary()
	tlfile, err := os.OpenFile(handler.tasklogname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("%s: message=%s: could not open task log %s: %v",
			handler.id, msg.ID, handler.tasklogname, err)
		msg.Done(false)
		return
	}
	defer tlfile.Close()

	log.Printf("%s: message=%s: process message", handler.id, msg.ID)
	cmd := exec.Command(handler.command, handler.args...)
	cmd.Stdout = tlfile
	cmd.Stderr = tlfile
	cmd.Stdin = bytes.NewReader(msg.Data)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		log.Printf("%s: message=%s: could not spawn command: %v", handler.id, msg.ID, err)
		msg.Done(false)
		return
	}

	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()
	select {
	case err := <-cmdDone:
		if err != nil {
			msg.Done(false)
			log.Printf("%s: message=%s: command failed: %v", handler.id, msg.ID, err)
		} else {
			msg.Done(true)
			log.Printf("%s: message=%s: command done", handler.id, msg.ID)
		}
	case <-time.After(handler.commandtimeout):
		log.Printf("%s: message=%s: kill process as command timeout reached", handler.id, msg.ID)
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-cmdDone:
			log.Printf("%s: message=%s: killed process", handler.id, msg.ID)
		case <-time.After(time.Second * 5):
			log.Printf("%s: message=%s: kill lingering process by SIGKILL", handler.id, msg.ID)
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
}
