PWD := $(shell pwd)

#
# Setup commands
#

# Build local Docker images
docker_build:
	make docker_hash > .dockerhash
	docker build -t commonsearch/local-back .

# Pull Docker images from the registry
docker_pull:
	docker pull commonsearch/local-back
	docker pull commonsearch/local-elasticsearch

# Build a unique hash to indicate that a docker_build may be necessary again
docker_hash:
	@sh -c 'cat Dockerfile requirements.txt | grep -vE "^\s*\#" | grep -vE "^\s*$$" | openssl md5'

# Build a source distribution, to be sent to a Spark cluster
build_source_export:
	rm -rf build/source_export*
	mkdir -p build/source_export
	cp -R cosrlib urlserver explainer jobs scripts tests requirements.txt Makefile build/source_export/
	cd build/source_export && tar --exclude='*/*.pyc' --exclude=".DS_Store" -czvf ../source_export.tgz * && cd ../../
	rm -rf build/source_export

# Downloads real data files and converts them to usable formats (RocksDB mostly)
import_local_data:

	# We have to use a temporary directory to build the data because virtualbox shares don't support directory fsync for rocksdb
	rm -rf local-data/urlserver-rocksdb
	rm -rf /tmp/cosrlocaldata
	mkdir -p /tmp/cosrlocaldata
	mkdir -p local-data
	COSR_PATH_LOCALDATA=/tmp/cosrlocaldata python urlserver/import.py  # -m cProfile -s cumtime
	mv /tmp/cosrlocaldata/urlserver-rocksdb local-data/urlserver-rocksdb
	rm -rf /tmp/cosrlocaldata

	if [ -z "$(COSR_TESTDATA)" ]; then ./scripts/import_commoncrawl.sh 0; fi

# Imports local mock data from tests
import_local_testdata:
	COSR_TESTDATA=1 make import_local_data

# Imports local mock data from tests in Docker
docker_import_local_testdata:
	 docker run -v "$(PWD):/cosr/back:rw" -w /cosr/back -i -t commonsearch/local-back make import_local_testdata

# Cleans the local source directories
clean: clean_coverage
	find . -name "*.pyc" | xargs rm -f
	find tests/ -name "__pycache__" | xargs rm -rf
	find cosrlib/ -name "*.so" | xargs rm -f
	find cosrlib/ -name "*.c" | xargs rm -f
	rm -rf .cache

# Removes all coverage data
clean_coverage:
	rm -f .coverage
	rm -f .coverage.*
	rm -rf htmlcov
	rm -f coverage.xml

# Creates a local Python virtualenv with all required modules installed
virtualenv:
	rm -rf venv
	virtualenv venv --distribute

	# Cython must be installed prior to gumbocy. TODO: how to fix that?
	grep -i Cython requirements.txt | xargs venv/bin/pip install

	venv/bin/pip install -r requirements.txt

# Compiles protocol buffer definitions
protoc:
	protoc -I urlserver/protos/ --python_out=urlserver/protos/ urlserver/protos/urlserver.proto

# Generates a small index for developement & uploads it to S3
devindex: restart_services
	sleep 30
	docker run -v "$(PWD):/cosr/back:rw" -w /cosr/back -i -t commonsearch/local-back python scripts/build_devindex.py --index --empty
	sleep 10
	docker exec -t -i `docker ps | awk '$$2=="commonsearch/local-elasticsearch" {print $$1}'` sh -c 'cd /usr/share/elasticsearch && tar zcf /tmp/cosr-es-devindex.tar.gz ./data'
	docker cp `docker ps | awk '$$2=="commonsearch/local-elasticsearch" {print $$1}'`:/tmp/cosr-es-devindex.tar.gz /tmp/
	s3cmd --acl-public --reduced-redundancy --multipart-chunk-size-mb=5 put /tmp/cosr-es-devindex.tar.gz s3://dumps.commonsearch.org/devindex/elasticsearch-data.tar.gz


#
# Day-to-day use commands
#

# Logins into a new container
docker_shell: docker_check docker_compose_prepare_containers
	docker-compose -p cosrbackshell down 2>/dev/null || true
	docker-compose -p cosrbackshell run --rm --service-ports cosrback bash
	docker-compose -p cosrbackshell down

# Logins into the same container again
docker_reshell:
	sh -c 'docker exec -t -i `docker ps | grep commonsearch/local-back | grep bash | cut -f 1 -d " "` bash'

# Stops all docker containers on this machine
docker_stop_all:
	bash -c 'docker ps -q | xargs docker stop -t=0'

# Cleans up leftover Docker images
docker_clean:
	docker rm -v $$(docker ps -a -q -f status=exited) || true
	docker rmi $$(docker images -aq) || true

# Checks if the container is out of date
docker_check:
	@bash -c 'docker run -v "$(PWD):/cosr/back:ro" -t commonsearch/local-back sh -c "if diff -q /cosr/.back-dockerhash /cosr/back/.dockerhash > /dev/null; then echo \"Docker image is up to date\"; else echo \"\nWARNING: Your Docker image seems to be out of date! Please exit and do \\\"make docker_build\\\" again to avoid any issues.\n\"; fi"'

start_local_elasticsearch:
	# sudo ifconfig lo0 alias 10.0.2.2
	elasticsearch -Dhttp.port=39200 -Dcluster.routing.allocation.disk.threshold_enabled=false -Dnetwork.host=0.0.0.0 -Dnode.local=true -Ddiscovery.zen.ping.multicast.enabled=false -Dnetwork.publish_host=10.0.2.2

# Reindex 1 WARC file from Common Crawl
reindex1:
	./scripts/elasticsearch_reset.py --delete
	spark-submit spark/jobs/pipeline.py --source commoncrawl:limit=1 --plugin plugins.filter.All:index_body=1 --profile

# Reindex 10 WARC files from Common Crawl
reindex10:
	./scripts/elasticsearch_reset.py --delete
	spark-submit spark/jobs/pipeline.py --source commoncrawl:limit=10 --plugin plugins.filter.All:index_body=1 --profile

# Do a standard reindex
reindex_standard:

	rm -rf ./out/
	./scripts/elasticsearch_reset.py --delete
	spark-submit --master local[4] --verbose --executor-memory 1G --driver-memory 512M spark/jobs/pipeline.py --source commoncrawl:limit=200,maxdocs=100,block=1 --plugin plugins.webgraph.DomainToDomainParquet:path=/cosr/back/out/

pagerank_standard:
	rm -rf ./out/pagerank/
	spark-submit --executor-memory 1G --driver-memory 1G spark/jobs/pagerank.py --gzip --webgraph /cosr/back/out/d2dgraph/ --dump /cosr/back/out/pagerank/ --maxiter 100 --shuffle_partitions 4 --stats 1 --tol -1 --precision 0.00001
	@echo ""
	@echo "Top 10 domains:"
	zcat out/pagerank/part*.gz | head

dump_standard:
	rm -rf ./out/
	spark-submit --verbose spark/jobs/pipeline.py --stop_delay 600 --source commoncrawl:limit=100,maxdocs=1000 --plugin plugins.dump.DocumentMetadata:format=parquet,path=./out/metadata,abort=1 --plugin plugins.webgraph.DomainToDomainParquet

viewdump_standard:
	hadoop jar /usr/spark/packages/jars/parquet-tools-1.8.1.jar cat --json ./out/metadata/

graph_standard:
	rm -rf out/d2dgraph
	spark-submit --verbose spark/jobs/pipeline.py --stop_delay 600 --source parquet:path=./out/metadata/ --plugin plugins.webgraph.DomainToDomainParquet:path=./out/d2dgraph,coalesce=10


# Run the explainer web service inside Docker
docker_explainer:
	docker run -v "$(PWD):/cosr/back:rw" -w /cosr/back -p 9703:9703  -e COSR_ELASTICSEARCHTEXT -e COSR_ELASTICSEARCHDOCS commonsearch/local-back python explainer/server.py


#
# Tests & code quality commands
#

# Runs all the tests (always from inside the container)
test: wait_for_elasticsearch import_local_testdata
	PYTHONDONTWRITEBYTECODE=1 py.test tests -v

# Runs all tests with coverage info
test_coverage: clean_coverage wait_for_elasticsearch
	COV_CORE_CONFIG=$(PWD)/.coveragerc COV_CORE_DATAFILE=$(PWD)/.coverage COV_CORE_SOURCE=$(PWD)/cosrlib:$(PWD)/urlserver:$(PWD)/spark make import_local_testdata
	PYTHONDONTWRITEBYTECODE=1 py.test --cov-append --cov=plugins --cov=cosrlib --cov=urlserver --cov=spark --cov-report html --cov-report xml --cov-report term tests/ -v
	mv .coverage.* .coverage
	coveralls || true

docker_test: docker_compose_prepare_containers
	docker-compose -p cosr-tests down 2>/dev/null || true
	docker-compose -p cosr-tests run --rm cosrback make test
	docker-compose -p cosr-tests down

docker_test_coverage: docker_compose_prepare_containers
	docker-compose -p cosr-tests run --rm cosrback make test_coverage
	docker-compose -p cosr-tests down 2>/dev/null || true
	docker-compose -p cosr-tests down

# TODO: check if docker-compose > v1.6 because we use the v2 compose format
docker_compose_prepare_containers:
	mkdir -p local-data/elasticsearch
	@docker-compose --version >> /dev/null || { \
		echo "docker-compose is not installed or is not in path :(\n  You can use 'pip install docker-compose' or see https://docs.docker.com/compose/install/#/install-docker-compose.\n"; \
		exit 1; \
	}

pylint:
	PYTHONPATH=. pylint -j 0 cosrlib urlserver spark explainer plugins
	PYTHONPATH=. pylint --py3k -j 0 cosrlib urlserver spark explainer plugins

docker_pylint:
	docker run -e "TERM=xterm-256color" --rm -t -v "$(PWD):/cosr/back:rw" -w /cosr/back commonsearch/local-back make pylint

todo:
	PYTHONPATH=. pylint -j 0 --disable=all --enable=fixme cosrlib urlserver spark explainer plugins

wait_for_elasticsearch:
	@echo "Waiting for elasticsearch..."
	while ! curl --output /dev/null --silent --head --fail http://elasticsearch:9200; do sleep 1 && echo -n .; done;
