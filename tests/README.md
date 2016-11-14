# Test suite for cosr-back

We have an extensive series of tests that are run on [Travis CI](https://travis-ci.org/commonsearch/cosr-back) at each commit.

## How to run everything

Just run this command:

```
make docker_test
```

This will start our dependencies like Elasticsearch (with docker-compose) and run the whole test suite in a container. This will take a while!

## How to run just one test

If you modified one part of the code and just want to run the associated tests again, you can open a shell into the container and invoke our test runner [pytest](https://pytest.org) directly.

First, open the shell:

```
make docker_shell
```

You are now inside the main container environment, with additional containers linked as dependencies available for your tests.

Then, run one of these commands depending on your needs:

```
# Just one directory
py.test tests/cosrlibtests/ -v

# Just one file, with realtime output
py.test tests/cosrlibtests/signals/test_wikidata.py -v -s

# By keyword
py.test tests/ -k "rank"

# Simple speed benchmark
py.test tests/cosrlibtests/document/html/ -v --repeat 50 --profile
```