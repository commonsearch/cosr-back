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

Now you have a local copy of the code and of the Docker images on your machine. Congratulations!



## 3. What to do next

You can now:

 - **Launch our test suite**, to make sure your install is working fine. See [tests/README.md](tests/README.md) for instructions.
 - **Run your first job** to analyze or index the data from Common Crawl. See our [tutorial](https://about.commonsearch.org/developer/tutorials/analyzing-the-web-with-spark-on-ec2).
 - **Contribute your first patch**, see [Developers: Get started](https://about.commonsearch.org/developer/get-started).
