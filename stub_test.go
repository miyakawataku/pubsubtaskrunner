package main

import (
	"log"
	"testing"
)

type testWriter struct {
	*testing.T
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.Logf("%s", p)
	return len(p), nil
}

func makeTestLogger(t *testing.T) *log.Logger {
	return log.New(testWriter{t}, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
}
