# Installing cosr-back on your local machine

This guide will take you through everything you need to do to have a local instance of `cosr-back` up and running.

Please note that we only support Linux and Mac OS X at the moment.



## 1. Install dependencies on your local machine

There are only 2 dependencies you need to install to get started:

- [Docker](http://docker.com) to run containers ([Installation instructions](https://docs.docker.com/engine/installation/))
- [git](http://git-scm.com) to interact with GitHub ([Installation instructions](https://help.github.com/articles/set-up-git/))

You should make sure your Docker daemon is started. Check the [Docker documentation](https://docs.docker.com/engine/installation/) if you are unsure how to start it. For instance on Ubuntu you may need to run `sudo service docker start`.



## 2. Download the code & the Docker images

To clone this repository from GitHub, go to your local workspace directory and run:

```
git clone https://github.com/commonsearch/cosr-back.git
cd cosr-back
```

Next, there are a few Docker images available that contain all the dependencies you will need to run `cosr-back`. To download them from the Docker Hub, just run:

```
[sudo] make docker_pull
```

Now you have a local copy of the code and of the Docker images on your machine!



## 3. Optional: Run the tests
Make sure to start the services (`[sudo] make start_services`) before trying any tests.

To make sure everything is okay, you should try to run the tests:

```
[sudo] make docker_test_coverage
```

This should take a few minutes as we run the tests.  Your output at the end might look like:

```
--------------- coverage: platform linux2, python 2.7.9-final-0 ----------------
Name                                         Stmts   Miss Branch BrPart  Cover
------------------------------------------------------------------------------
cosrlib/__init__.py                              4      0      0      0   100%
cosrlib/config.py                               12      3      4      1    75%
cosrlib/document/__init__.py                    86     14     20      5    80%
cosrlib/document/html/__init__.py                1      0      0      0   100%
cosrlib/document/html/defs.py                   15      0      0      0   100%
cosrlib/document/html/htmldocument.py           54      1     16      1    97%
cosrlib/document/html/htmlencoding.py           70      1     42      7    93%
cosrlib/document/html/parsers.py                 4      0      0      0   100%
cosrlib/es.py                                   70     11     26      7    79%
...
------------------------------------------------------------------------------
TOTAL                                         1893    316    662    107    81%
Coverage HTML written to dir htmlcov
Coverage XML written to file coverage.xml

==================== 78 passed, 1 skipped in 787.97 seconds ====================
```


## 4. Optional: Index your first Common Crawl segment

### Background on the Common Crawl Dataset
Common Crawl takes a **snapshot** of the internet approximately every month.  Since all of the data is very, very, large the data is split up into **segments**.  All of these segments are hosted on by Amazon S3 as a part of their very generous Public Datasets program.

If you're curious where you can find the URL's to the segments, visit the [Common Crawl Website](http://commoncrawl.org/the-data/get-started/).  If you click on a specific month, you can find a link to download all of the WARC paths.  Each path inside of this WARC paths file is a link to a Common Crawl Segment.  **Don't worry, CSR-back makes it easy to download segments from Common Crawl ... we will help you download your first segment in the section below.**

Note: While experimenting locally, as a general practice, it is recommended to limit the number of segment URL's that you are processing from to 1 or 2, to avoid overloading your own computer.  Each segment can be ~ 500 mb or more.

### Indexing your first Segment
Let's play with real data! We provide an easy way to index a single Common Crawl segment.

Switch into your account on the Docker machine by entering:

```
[sudo] make docker_shell
```

Let's import some static ranking data first:

```
make import_local_data
```

Then you should start the local services like Elasticsearch and the URLserver:

```
make start_services
```

Finally, you should be able to download (500MB+) and index the first Common Crawl segment:

```
make reindex1
```

Congratulations! You should now be able to start diving further in the code of `cosr-back`.