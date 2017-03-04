package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

type opt struct {
	command        string
	args           []string
	project        string
	subscription   string
	credentials    string
	parallelism    int
	retrytimeout   time.Duration
	commandtimeout time.Duration
}

func parseOpt() opt {
	opt := opt{}
	flag.StringVar(&opt.project, "project", "",
		"project ID of the topic/subscription (currently required)")
	flag.StringVar(&opt.subscription,
		"subscription", "", "subscription ID (required)")
	flag.StringVar(&opt.credentials, "credentials", "",
		"path to service account credentials (currently required)")
	flag.IntVar(&opt.parallelism, "parallelism", 1,
		"maximum number of tasks executed in parallel")
	flag.DurationVar(&opt.retrytimeout, "retrytimeout", time.Minute*120,
		"maximum duration from publishing until last retry")
	flag.DurationVar(&opt.commandtimeout, "commandtimeout", time.Second*60,
		"maximum duration of a single command execution")
	flag.Parse()

	if opt.project == "" {
		fmt.Fprintf(os.Stderr, "--project required\n")
		os.Exit(1)
	}
	if opt.subscription == "" {
		fmt.Fprintf(os.Stderr, "--subscription required\n")
		os.Exit(1)
	}
	if opt.credentials == "" {
		fmt.Fprintf(os.Stderr, "--credentials required\n")
		os.Exit(1)
	}
	if opt.parallelism < 1 {
		fmt.Fprintf(os.Stderr, "--parallelism requires positive number")
		os.Exit(1)
	}
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "command required\n")
		os.Exit(1)
	}
	opt.command = flag.Arg(0)
	opt.args = flag.Args()[1:]
	return opt
}
