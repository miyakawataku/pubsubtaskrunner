package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// conf is a struct of the program configuration.
type conf struct {
	command        string
	args           []string
	project        string
	subscription   string
	credentials    string
	parallelism    int
	tasklogdir     string
	maxtasklogkb   int
	taskttl        time.Duration
	commandtimeout time.Duration
	termtimeout    time.Duration
}

// readConf reads configuration from the commandline.
func readConf() conf {
	conf := conf{}
	flag.StringVar(&conf.project, "project", "",
		"project ID of the topic/subscription (currently required)")
	flag.StringVar(&conf.subscription,
		"subscription", "", "subscription ID (required)")
	flag.StringVar(&conf.credentials, "credentials", "",
		"path to service account credentials (currently required)")
	flag.IntVar(&conf.parallelism, "parallelism", 1,
		"maximum number of tasks executed in parallel")
	flag.StringVar(&conf.tasklogdir, "tasklogdir", ".",
		"task log directory")
	flag.IntVar(&conf.maxtasklogkb, "maxtasklogkb", 1000,
		"size in KB per task log file, which triggres log rotation")
	flag.DurationVar(&conf.taskttl, "taskttl", time.Minute*120,
		"TTL (time-to-live) duration of the task")
	flag.DurationVar(&conf.commandtimeout, "commandtimeout", time.Second*60,
		"timeout duration of a single command execution")
	flag.DurationVar(&conf.termtimeout, "termtimeout", time.Second*5,
		"timeout duration of the first command termination attempt by SIGTERM")
	flag.Parse()

	if conf.project == "" {
		fmt.Fprintf(os.Stderr, "--project required\n")
		os.Exit(1)
	}
	if conf.subscription == "" {
		fmt.Fprintf(os.Stderr, "--subscription required\n")
		os.Exit(1)
	}
	if conf.credentials == "" {
		fmt.Fprintf(os.Stderr, "--credentials required\n")
		os.Exit(1)
	}
	if conf.parallelism < 1 {
		fmt.Fprintf(os.Stderr, "--parallelism requires positive number")
		os.Exit(1)
	}
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "command required\n")
		os.Exit(1)
	}
	conf.command = flag.Arg(0)
	conf.args = flag.Args()[1:]
	return conf
}
