# cosr-back

[![Build Status](https://travis-ci.org/commonsearch/cosr-back.svg?branch=master)](https://travis-ci.org/commonsearch/cosr-back) [![Coverage Status](https://coveralls.io/repos/github/commonsearch/cosr-back/badge.svg?branch=master)](https://coveralls.io/github/commonsearch/cosr-back?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/commonsearch/cosr-back.svg)](LICENSE)

This repository contains the main components of the [Common Search](https://about.commonsearch.org) backend.

Your help is welcome! We have a complete guide on [how to contribute](CONTRIBUTING.md).

## Understand the project

This repository has 4 components:

 - **cosrlib**: Python code for parsing, analyzing and indexing documents
 - **jobs**: Spark jobs using cosrlib.
 - **urlserver**: A service for getting metadata about URLs from static databases
 - **explainer**: A web service for explaining and debugging results, hosted at [explain.commonsearch.org](https://explain.commonsearch.org/)

Here is how they fit in our [general architecture](https://about.commonsearch.org/developer/architecture):

![General technical architecture of Common Search](https://about.commonsearch.org/images/developer/architecture-2016-02.svg)

## Local install with Docker

Running `cosr-back` on your local machine is very simple. You only need to have [Docker](https://docs.docker.com/engine/installation/) installed.

Once Docker is launched, just run:

```
make start_services
make docker_shell
```

You will enter the main container with the ability to run the tests or launch Spark jobs.

## Launching the tests

Make sure to start the services (`make start_services`) before trying any tests.

Inside Docker, you can run our full test suite easily:

```
make test
```

Alternatively, you can run it from outside Docker with:

```
make docker_test
```

You may also want to run only part of the tests, for instance all which do not use Elasticsearch:

```
py.test tests/ -v -m "not elasticsearch"
```

If you want to evaluate the speed of a component, for instance HTML parsing, you can repeat the tests N times and output a Python profile:

```
py.test tests/cosrlibtests/document/html/ -v --repeat 50 --profile
```

## Launching an index job

```
./scripts/import_commoncrawl.sh 0
spark-submit jobs/spark/index.py --warc_limit 1 --only_homepages --profile
```

After this, if you have a `cosr-front` instance connected to the same Elasticsearch service, you will see the results!

## Launching the explainer

The explainer allows you to debug results easily. Just run:

```
make docker_explainer
```

Then open [http://192.168.99.100:9703](http://192.168.99.100:9703) in your browser (Assuming `192.168.99.100` is the IP of your Docker host)

## Alternate install without Docker

If for some reason you don't want to use Docker, you might be able to use your local Python install to run `cosr-back`. Please note that this is an unsupported method and might break at any time.

You would have to install the dependencies manually: Spark (symlinked in ./spark/), rocksdb, gumbo. Then:

```
make virtualenv
source venv/bin/activate
make import_local_testdata
make test
```
