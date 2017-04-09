package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

// taskHandlerConf contains configuration of a taskHandler.
type taskHandlerConf struct {
	// id is the ID of the handler.
	id string

	// command is the command to execute.
	command string

	// args is the arguments given to the command.
	args []string

	// commandtimeout is the maximum duration till the handler
	// gives up the completion of the command.
	// When the timeout has been reached, SIGTERM is sent to the process group
	// of the command, and the task attempt is marked as failure.
	commandtimeout time.Duration

	// termtimeout is the maximum duration till the handler
	// gives up the command completion after SIGTERM is sent by commandtimeout.
	// When the timeout has been reached, SIGKILL is sent to the process group
	// of the command.
	termtimeout time.Duration

	// taskttl is the time-to-live (TTL) duration of a task since the publish time.
	// If the TTL has passed, the message is acked because of expiration.
	taskttl time.Duration

	// reqCh is the channel of requests to the puller.
	reqCh chan<- struct{}

	// respCh is the channel of messages from the puller.
	respCh <-chan *pubsub.Message

	// doneCh is the channel to notify that the handler is shut down gracefully.
	doneCh chan<- struct{}

	// tasklogpath is the path of the task log file of the handler.
	tasklogpath string

	// maxtasklogkb is the maximum size of the task log file in KB.
	maxtasklogkb int
}

// taskHandler executes the command with a message content as the standard input.
type taskHandler struct {
	taskHandlerConf

	// now is Time.now.
	now func() time.Time

	handleSingleTask handleSingleTaskFunc
	openTaskLog      openTaskLogFunc
	rotateTaskLog    rotateTaskLogFunc
	runCmd           runCmdFunc
}

// makeHandler makes a handler from the configuration.
func makeHandler(hc taskHandlerConf) *taskHandler {
	return &taskHandler{
		taskHandlerConf:  hc,
		now:              time.Now,
		handleSingleTask: handleSingleTask,
		openTaskLog:      openTaskLog,
		rotateTaskLog:    rotateTaskLog,
		runCmd:           runCmd,
	}
}

// handleTillShutdown handles messages sent from the puller.
// The method returns when the context is shutdown.
func (handler *taskHandler) handleTillShutdown(ctx context.Context) {
	log.Printf("%s: start", handler.id)
	for {
		handler.reqCh <- struct{}{}
		select {
		case msg := <-handler.respCh:
			notifier := handler.handleSingleTask(handler, msg)
			notifier.notify(handler, msg)
		case <-ctx.Done():
			log.Printf("%s: shutdown", handler.id)
			handler.doneCh <- struct{}{}
			return
		}
	}
}

// msgNotifier is an interface to call Message.Done(bool).
type msgNotifier interface {
	notify(handler *taskHandler, msg *pubsub.Message)
}

// fixedMsgNotifier is a struct to call Message.Done(bool).
type fixedMsgNotifier struct {
	isAcked bool
	desc    string
}

// notify acks or nacks the message.
func (mn *fixedMsgNotifier) notify(handler *taskHandler, msg *pubsub.Message) {
	log.Printf("%s: message=%s: %s the message", handler.id, msg.ID, mn.desc)
	msg.Done(mn.isAcked)
}

var (
	// ack calls Message.Done(true).
	ack msgNotifier

	// nack calls Message.Done(true).
	nack msgNotifier
)

func init() {
	ack = &fixedMsgNotifier{
		isAcked: true,
		desc:    "ack",
	}
	nack = &fixedMsgNotifier{
		isAcked: false,
		desc:    "nack",
	}
}

// handleSingleTaskFunc is the type of handleSingleTask function.
type handleSingleTaskFunc func(handler *taskHandler, msg *pubsub.Message) msgNotifier

// handleSingleTask handles a message as a task, then returns ack or nack.
func handleSingleTask(handler *taskHandler, msg *pubsub.Message) msgNotifier {

	// if the TTL has passed, ack the task because of expiration.
	taskDeadline := msg.PublishTime.Add(handler.taskttl)
	if handler.now().After(taskDeadline) {
		log.Printf("%s: message=%s: delete task because of exceeded retry deadline %v",
			handler.id, msg.ID, taskDeadline.In(time.Local))
		return ack
	}

	if _, err := handler.rotateTaskLog(handler); err != nil {
		log.Printf("%s: message=%s: could not rotate task log %s: %v",
			handler.id, msg.ID, handler.tasklogpath, err)
		return nack
	}

	taskLog, err := handler.openTaskLog(handler)
	if err != nil {
		log.Printf("%s: message=%s: could not open task log %s: %v",
			handler.id, msg.ID, handler.tasklogpath, err)
		return nack
	}
	defer taskLog.Close()

	if err := handler.runCmd(handler, msg, taskLog); err != nil {
		log.Printf("%s: message=%s: command failed: %v", handler.id, msg.ID, err)
		return nack
	}

	log.Printf("%s: message=%s: command done", handler.id, msg.ID)
	return ack
}

// rotateTaskLogFunc is the type of rotateTaskLog function.
type rotateTaskLogFunc func(handler *taskHandler) (rotated bool, err error)

// rotateTaskLog renames the task log to "<tasklogpath>.prev",
// if the size exceeds maxtasklogkb.
func rotateTaskLog(handler *taskHandler) (rotated bool, err error) {
	stat, err := os.Stat(handler.tasklogpath)
	switch {
	case os.IsNotExist(err):
		log.Printf("%s: create new task log %s", handler.id, handler.tasklogpath)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("cannot stat %s: %v", handler.tasklogpath, err)
	}

	if stat.Size() <= int64(handler.maxtasklogkb*1000) {
		return false, nil
	}

	prevLogPath := handler.tasklogpath + ".prev"
	log.Printf("%s: mv task log %s to %s", handler.id, handler.tasklogpath, prevLogPath)
	if err := os.Rename(handler.tasklogpath, prevLogPath); err != nil {
		return false, fmt.Errorf("could not mv task log %s to %s: %v", handler.tasklogpath, prevLogPath, err)
	}

	return true, nil
}

// openTaskLogFunc is the type of openTaskLog function.
type openTaskLogFunc func(handler *taskHandler) (io.WriteCloser, error)

// openTaskLog opens the task log to append.
func openTaskLog(handler *taskHandler) (io.WriteCloser, error) {
	return os.OpenFile(handler.tasklogpath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
}

// spawnError describes a problem spawning the command.
type spawnError struct {
	cause error
}

// Error returns the string representation of a spawnError.
func (err spawnError) Error() string {
	return fmt.Sprintf("could not spawn command: %v", err.cause)
}

// cmdTimeoutError represents an error that the commandtimeout has been reached.
type cmdTimeoutError int

// Error returns the string represents of a cmdTimeoutError.
func (pgid cmdTimeoutError) Error() string {
	return fmt.Sprintf("command timeout: command-pgid=%d", pgid)
}

// cmdTermTimeoutError represents an error that the termtimeout has been reached.
type cmdTermTimeoutError int

// Error returns the string representation of a cmdTermTimeoutError.
func (pgid cmdTermTimeoutError) Error() string {
	return fmt.Sprintf("command termination timeout: command-pgid=%d", pgid)
}

// runCmdFunc is the type of runCmd function.
type runCmdFunc func(handler *taskHandler, msg *pubsub.Message, taskLog io.Writer) error

// runCmd executes a command with the message content as the stdin,
// and the task log as the stdout and the stderr.
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
