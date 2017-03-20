# pubsubtaskrunner

A task runner program which polls tasks from [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/),
and execute an external program giving the message content as the stdin.

The software is under development. There is not yet any release.

## Usage

The commandline below polls `yoursubs` subscription,
and executes `tee` command for each message to append the message content
to the file `msgcontents.data`.

    $ pubsubtaskrunner \
        --project yourpj \
        --subscription yoursubs \
        --credentials /path/to/credentials.json \
        /bin/tee -a msgcontents.data

To use the service account related to your Google Compute Engine instance to connect to Pub/Sub,
just omit `--credentials` option.
When the option is omitted,
the [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials)
is used.

    $ pubsubtaskrunner \
        --project yourpj \
        --subscription yoursubs \
        /bin/tee -a msgcontents.data

There are other options such as `--parallelism` and `--taskttl`.
Too see the description of the options, try `pubsubtaskrunner --help`.

## License

The software is distributed under the MIT Licence.

See LICENSE.txt for detail.

<!-- vim: set et sw=2 sts=2 ft=markdown : -->
