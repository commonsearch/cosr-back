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

Now you have a local copy of the code and of the Docker images on your machine!



## 3. Optional: Run the tests

To make sure everything is okay, you should try to run the tests:

```
make docker_test_coverage
```


## 4. Optional: Index your first Common Crawl segment

Let's play with real data! We provide an easy way to index a single Common Crawl segment.

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