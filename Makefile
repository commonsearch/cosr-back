PWD := $(shell pwd)

#
# Setup commands
#

# Build local Docker images
docker_build:
	docker build -t commonsearch/local-back .

# Pull Docker images from the registry
docker_pull:
	docker pull commonsearch/local-back
	docker pull commonsearch/local-elasticsearch

# Build a source distribution, to be sent to a Spark cluster
build_source_export:
	rm -rf build/source_export*
	mkdir -p build/source_export
	cp -R cosrlib urlserver jobs scripts tests requirements.txt Makefile build/source_export/
	cd build/source_export && tar --exclude='*/*.pyc' --exclude=".DS_Store" -czvf ../source_export.tgz * && cd ../../
	rm -rf build/source_export

# Downloads real data files and converts them to usable formats (RocksDB mostly)
import_local_data:
	python scripts/import_alexa_top1m.py
	python scripts/import_dmoz.py
	python scripts/import_webdatacommons_hc.py
	python scripts/import_ut1_blacklist.py
	./scripts/import_commoncrawl.sh 0

# Imports local mock data from tests
import_local_testdata:
	COSR_TESTDATA=1 make import_local_data

# Cleans the local source directories
clean:
	find . -name "*.pyc" | xargs rm -f
	find tests/ -name "__pycache__" | xargs rm -rf
	find cosrlib/ -name "*.so" | xargs rm -f
	find cosrlib/ -name "*.c" | xargs rm -f
	rm -rf .cache

# Creates a local Python virtualenv with all required modules installed
virtualenv:
	rm -rf venv
	virtualenv venv --distribute

	# Cython must be installed prior to gumbocy. TODO: how to fix that?
	grep -i Cython requirements.txt | xargs venv/bin/pip install

	venv/bin/pip install -r requirements.txt



#
# Day-to-day use commands
#

# Logins into the container
docker_shell:
	docker run -v "$(PWD):/cosr/back:rw" -w /cosr/back -i -t commonsearch/local-back bash

# Stops all docker containers on this machine
docker_stop_all:
	bash -c 'docker ps | tail -n +2 | cut -d " " -f 1 | xargs docker stop -t=0'

# Starts local services
start_services:
	mkdir -p local-data/elasticsearch

	# ElasticSearch
	docker run -d -p 39200:9200 -p 39300:9300 commonsearch/local-elasticsearch

	# URLServer
	docker run -d -v "$(PWD):/cosr/back:rw" -w /cosr/back -p 9702:9702 commonsearch/local-back python urlserver/server.py

# Stops local services
stop_services:
	bash -c 'docker ps | tail -n +2 | grep -E "((commonsearch/local-elasticsearch)|(urlserver))" | cut -d " " -f 1 | xargs docker stop -t=0'

# Restarts local services
restart_services: stop_services start_services

# Reindex 1 WARC file from Common Crawl
reindex1:
	./scripts/elasticsearch_reset.py --delete
	spark-submit jobs/spark/index.py --warc_limit 1

# Reindex 10 WARC files from Common Crawl
reindex10:
	./scripts/elasticsearch_reset.py --delete
	spark-submit jobs/spark/index.py --warc_limit 10



#
# Tests & code quality commands
#

# Runs all the tests
test:
	PYTHONDONTWRITEBYTECODE=1 py.test tests -v

pylint:
	PYTHONPATH=. pylint cosrlib urlserver jobs

todo:
	PYTHONPATH=. pylint --disable=all --enable=fixme cosrlib urlserver jobs
