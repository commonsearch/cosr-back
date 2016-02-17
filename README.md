# cosr-back

This repository contains the main components of the [Common Search](https://about.commonsearch.org) backend, basically everything happening before indexing in Elasticsearch:

 - **cosrlib**: Python code for parsing, analyzing and indexing documents
 - **jobs**: Spark jobs using cosrlib.
 - **urlserver**: A service for getting metadata about URLs from static databases
 - **explainer**: (Upcoming) A web frontend for explaining and debugging results

Your help is welcome! You can use the [issues page](https://github.com/commonsearch/cosr-back) to suggest improvements or report bugs.

## Local install with Docker

Running `cosr-back` on your local machine is very simple. You only need to have [Docker](https://docs.docker.com/engine/installation/) installed.

Once Docker is launched, just run:

```
make start_services
make docker_shell
```

You will enter the main container with the ability to run the tests or launch Spark jobs.

## Launching the tests

Only once, you need to import some mock data:

```
make import_local_testdata
```

Then you can run our full test suite:

```
make test
```

## Launching an index job

```
spark-submit jobs/spark/index.py --warc_limit 1 --only_homepages --profile
```

After this, if you have a `cosr-front` instance connected to the same Elasticsearch service, you will see the results!

## Alternate install without Docker

If for some reason you don't want to use Docker, you might be able to use your local Python install to run `cosr-back`. Please note that this is an unsupported method and might break at any time.

You would have to install the dependencies manually: Spark (symlinked in ./spark/), rocksdb, gumbo. Then:

```
make virtualenv
source venv/bin/activate
make import_local_testdata
make test
```
