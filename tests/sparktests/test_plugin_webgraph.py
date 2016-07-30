import pytest
import os
from test_index import CORPUSES
import tempfile
import shutil
import pipes
import ujson as json
import subprocess


def _validate_txt_graph(webgraph_dir):

    # We collect(1) so there should be only one partition
    webgraph_file = webgraph_dir + "/out/part-00000"
    assert os.path.isfile(webgraph_file)

    with open(webgraph_file, "r") as f:
        graph = [x.split(" ") for x in f.read().strip().split("\n")]

    print graph
    assert len(graph) == 3
    assert ["example-a.com", "example-b.com"] in graph
    assert ["example-b.com", "example-c.com"] in graph
    assert ["example-c.com", "example-b.com"] in graph


def _read_parquet(parquet_path):
    out = subprocess.check_output("hadoop jar /usr/lib/parquet-tools-1.8.1.jar cat --json %s 2>/dev/null" % parquet_path, shell=True)
    return [json.loads(line) for line in out.strip().split("\n")]


@pytest.mark.elasticsearch
def test_spark_link_graph_txt(indexer, sparksubmit):

    webgraph_dir = tempfile.mkdtemp()

    try:

        sparksubmit("spark/jobs/index.py  --plugin plugins.filter.All:parse=1,index=0  --source wikidata --source corpus:%s  --plugin plugins.webgraph.DomainToDomain:coalesce=1,path=%s/out/" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            webgraph_dir
        ))

        _validate_txt_graph(webgraph_dir)

    finally:
        shutil.rmtree(webgraph_dir)


def test_spark_link_graph_txt_with_intermediate_dump(sparksubmit):
    """ Test intermediate dump generation & parquet source,
         + having no dependency on elasticsearch when not actually indexing
    """

    webgraph_dir = tempfile.mkdtemp()

    try:

        # Generate temporary dump
        sparksubmit("spark/jobs/index.py --source corpus:%s --plugin plugins.filter.All:parse=1,index=0 --plugin plugins.dump.DocumentMetadataParquet:path=%s/intermediate/,abort=1 --plugin plugins.webgraph.DomainToDomain:coalesce=1,path=%s/out/" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            webgraph_dir,
            webgraph_dir
        ))

        assert not os.path.isdir("%s/out/" % webgraph_dir)

        print "Intermediate file dump:"

        print _read_parquet("%s/intermediate/" % webgraph_dir)

        # Resume pipeline from that dump
        sparksubmit("spark/jobs/index.py --source parquet:path=%s/intermediate/ --plugin plugins.webgraph.DomainToDomain:coalesce=1,path=%s/out/ --plugin plugins.filter.All:parse=1,index=0" % (
            webgraph_dir,
            webgraph_dir
        ))

        _validate_txt_graph(webgraph_dir)

    finally:
        shutil.rmtree(webgraph_dir)


@pytest.mark.elasticsearch
def test_spark_link_graph_parquet(indexer, sparksubmit):

    webgraph_dir = tempfile.mkdtemp()

    try:

        domain_a_id = indexer.client.urlclient.get_domain_id("http://example-a.com/")
        domain_b_id = indexer.client.urlclient.get_domain_id("http://example-b.com/")
        domain_c_id = indexer.client.urlclient.get_domain_id("http://example-c.com/")
        domain_d_id = indexer.client.urlclient.get_domain_id("http://example-d.com/")

        sparksubmit("spark/jobs/index.py  --plugin plugins.filter.All:parse=1,index=0 --source corpus:%s  --plugin plugins.webgraph.DomainToDomainParquet:coalesce=1,path=%s/out/" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            webgraph_dir
        ))

        # Then read the generated Parquet files with another library to ensure compatibility
        # TODO: replace this with a JSON dump from a Python binding when available
        lines = _read_parquet("%s/out/edges/" % webgraph_dir)

        for src, dst in [
            (domain_a_id, domain_b_id),
            (domain_b_id, domain_c_id),
            (domain_c_id, domain_b_id)
        ]:
            assert {"src": src, "dst": dst, "weight": 1.0} in lines

        assert len(lines) == 3

        lines = _read_parquet("%s/out/vertices/" % webgraph_dir)

        assert {"id": domain_a_id, "domain": "example-a.com"} in lines
        assert {"id": domain_b_id, "domain": "example-b.com"} in lines
        assert {"id": domain_c_id, "domain": "example-c.com"} in lines
        assert {"id": domain_d_id, "domain": "example-d.com"} in lines

        assert len(lines) == 4

    finally:
        shutil.rmtree(webgraph_dir)
