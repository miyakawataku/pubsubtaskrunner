# pubsubtaskrunner

A task runner program which polls tasks from [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/),
and execute an external program giving the message content as the stdin.

The software is under development. There is not yet any release.

## Usage

The commandline goes like this:

    pubsubtaskrunner [OPTION...] COMMAND [ARGUMENTS...]

### Options

* -project String (requied)

  * The project ID of the Pub/Sub subscription.

* -subscription String (required)

  * The Pub/Sub subscription ID.

* -credentials Path-to-file (optional)

  * The service account credentials file to access Pub/Sub resources.
  * Default to the [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).

* -parallelism Integer (default = 1)

  * The maximum number of tasks executed in parallel.
  * Default to 1.

* -tasklogdir Path-to-directory (default = .)

  * The task log directory, which task log files are placed on.
  * Default to “.” (the current directory).

* -maxtasklogkb Integer (default = 1000)

  * The size in Kilobytes per task log file, which triggers log rotation.
  * Default to 1000 (1 Megabytes).

* -taskttl Duration-of-time (default = 120m)

  * The task TTL, which is the time-to-live duration of the task.
  * Default to 120m (2 hours).

* -commandtimeout Duration-of-time (default = 60s)

  * The timeout of a single command execution.
  * Default to 60s (1 minute).

* -tertimeout Duration-of-time (default = 5s)

  * The timeout of the first command termination attempt by SIGTERM.
  * Default to 5s (5 seconds).

### Task attempts

The pubsubtaskrunner program waits messages from a Pub/Sub subscription.
When a message has reached, it spawns COMMAND with ARGUMENTS,
giving the content of the message as the stdin.

If the spawned subprocess terminates with exit status 0,
the message will be acked.

If the subprocess does not terminate with a nonzero exit status,
the message will be nacked.

If the subprocess does not terminate within the command timeout (-commandtimeout option),
the _process group_ of the subprocess will be SIGTERMed and the message will be nacked.
If the subprocess does not terminate after SIGTERM within the termination timeout (-termtimeout option),
the process group of the subprocess will be SIGKILLed and the message will be nacked.

If the received message is older than the publish time + the task TTL (-termtimeout option),
the message will be _acked_ so that the message won't be scheduled again.

### Output

The log of the program itself is written to the stderr.

The stdout and the stderr of subprocesses are written to _task log files_.
Task log files are placed on the task log directory (-tasklogdir option),
and named task0.log, task1.log, task2.log, ... taskN.log, where N is (parallelism - 1).
Output of multiple tasks may be included in a single task log file.
When the size of a task log file exceeds the threshold (-maxtasklogkb option),
the current file is renamed to task0.log.prev, task1.log.prev, ... or taskN.log.prev,
and the new task log file is created.

### Shutdown

The pubsubtaskrunner program starts a graceful shutdown
when SIGINT (Ctrl+C) or SIGTERM (kill command default) is sent.

When the shutdown is requested,
pubsubtaskrunner stops to receive messages.
After all the ongoing task attempts are acked or nacked,
the pubsubtaskrunner program halts.

### Examples

The commandline below polls `yoursubs` subscription,
and executes `tee` command for each message to append the message content
to the file `msgcontents.data`.

    $ pubsubtaskrunner \
        -project your-project \
        -subscription your-subs \
        -credentials /path/to/credentials.json \
        /bin/tee -a msgcontents.data

To use the
[Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials),
such as the service account related to your Google Compute Engine instance, to connect to Pub/Sub,
just omit `--credentials` option.

    $ pubsubtaskrunner \
        -project your-project \
        -subscription your-subs \
        /bin/tee -a msgcontents.data

## Environment

This program should run on Unix like environments, such as GNU/Linux and Mac OS X.

Windows is not supported, because the program utilizes process groups and signals
to manage subprocesses.

## Building

To build the binary, type:

    $ go build

To install the binary, type:

    $ go install

To test, type:

    $ go test

Integration tests are tagged as `integration`.
Running integration tests require your GCP project ID in `-project` option,
Pub/Sub topic ID in `-topic` option,
Pub/Sub subscription ID in `-subscription` option,
and service account credentials file to access them in `-credentials` option.
The subscription must be related with the topic.
Hence the commandline goes like this:

    $ go test -tags integration -args \
        -project your-project \
        -topic your-topic \
        -subscription your-subs \
        -credentials /path/to/credentials.json

To use the [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials)
for integration tests, omit `-credentials` option like this:

    $ go test -tags integration -args \
        -project your-project \
        -topic your-topic \
        -subscription your-subs

## License

The software is distributed under the MIT Licence.

See LICENSE.txt for detail.

<!-- vim: set et sw=2 sts=2 ft=markdown : -->
