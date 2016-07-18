import pytest
import os
from test_index import CORPUSES
import tempfile
import shutil
import pipes
import ujson as json
import subprocess


@pytest.mark.elasticsearch
def test_spark_link_graph_txt(indexer, sparksubmit):

    linkgraph_dir = tempfile.mkdtemp()

    try:

        sparksubmit("spark/jobs/index.py --source corpus:%s  --plugin plugins.linkgraph.DomainToDomain:coalesce=1,dir=%s/out/" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            linkgraph_dir
        ))

        # We collect(1) so there should be only one partition
        linkgraph_file = linkgraph_dir + "/out/part-00000"
        assert os.path.isfile(linkgraph_file)

        with open(linkgraph_file, "r") as f:
            graph = [x.split(" ") for x in f.read().strip().split("\n")]

        print graph
        assert len(graph) == 3
        assert ["example-a.com", "example-b.com"] in graph
        assert ["example-b.com", "example-c.com"] in graph
        assert ["example-c.com", "example-b.com"] in graph

    finally:
        shutil.rmtree(linkgraph_dir)


@pytest.mark.elasticsearch
def test_spark_link_graph_parquet(indexer, sparksubmit):

    linkgraph_dir = tempfile.mkdtemp()

    try:

        domain_a_id = indexer.client.urlclient.get_domain_id("http://example-a.com/")
        domain_b_id = indexer.client.urlclient.get_domain_id("http://example-b.com/")
        domain_c_id = indexer.client.urlclient.get_domain_id("http://example-c.com/")
        domain_d_id = indexer.client.urlclient.get_domain_id("http://example-d.com/")

        sparksubmit("spark/jobs/index.py --source corpus:%s  --plugin plugins.linkgraph.DomainToDomainParquet:coalesce=1,dir=%s/out/" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            linkgraph_dir
        ))

        # Then read the generated Parquet files with another library to ensure compatibility
        # TODO: replace this with a JSON dump from a Python binding when available
        out = subprocess.check_output("hadoop jar /usr/spark/packages/parquet-tools-1.8.1.jar cat --json %s/out/edges/ 2>/dev/null" % linkgraph_dir, shell=True)

        print repr(out)

        lines = [json.loads(line) for line in out.strip().split("\n")]

        for src, dst in [
            (domain_a_id, domain_b_id),
            (domain_b_id, domain_c_id),
            (domain_c_id, domain_b_id)
        ]:
            assert {"src": src, "dst": dst} in lines

        assert len(lines) == 3

        out = subprocess.check_output("hadoop jar /usr/spark/packages/parquet-tools-1.8.1.jar cat --json %s/out/vertices/ 2>/dev/null" % linkgraph_dir, shell=True)

        print repr(out)

        lines = [json.loads(line) for line in out.strip().split("\n")]

        assert {"id": domain_a_id, "domain": "example-a.com"} in lines
        assert {"id": domain_b_id, "domain": "example-b.com"} in lines
        assert {"id": domain_c_id, "domain": "example-c.com"} in lines
        assert {"id": domain_d_id, "domain": "example-d.com"} in lines

        assert len(lines) == 4

    finally:
        shutil.rmtree(linkgraph_dir)

