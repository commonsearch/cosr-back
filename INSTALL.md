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
make docker_pull
```

Linux users: depending on your Docker install you make get a permission error with the command above. To fix it you should prefix all the docker commands by `sudo`, for instance: `sudo make docker_pull`.

Now you have a local copy of the code and of the Docker images on your machine!



## 3. Optional: Run the tests

To make sure everything is okay, you can try to run the tests! See our [README.md](/README.md) for instructions.


## 4. Optional: Index your first Common Crawl segment

### Background on the Common Crawl Dataset

Common Crawl takes a **snapshot** of the internet approximately every month.  Since all of the data is very, very, large the data is split up into **segments**.  All of these segments are hosted on by Amazon S3 as a part of their Public Datasets program.

If you're curious where you can find the URL's to the segments, visit the [Common Crawl Website](http://commoncrawl.org/the-data/get-started/).  If you click on a specific month, you can find a link to download all of the WARC paths.  Each path inside of this WARC paths file is a link to a Common Crawl Segment.  **Don't worry, cosr-back makes it easy to download segments from Common Crawl ... we will help you download your first segment in the section below.**

Note: While experimenting locally, as a general practice, it is recommended to limit the number of segment URL's that you are processing from to 1 or 2, to avoid overloading your own computer.  Each segment can be ~ 500 mb or more.

### Indexing your first Segment

Let's play with real data! We provide an easy way to index a single Common Crawl segment.

Switch into your account on the Docker machine by entering:

```
make docker_shell
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
