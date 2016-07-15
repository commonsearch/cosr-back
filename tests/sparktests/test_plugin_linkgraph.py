import pytest
import os
from test_index import CORPUSES
import tempfile
import shutil
import pipes
import ujson as json


@pytest.mark.elasticsearch
def test_spark_link_graph(indexer, sparksubmit):

    linkgraph_dir = tempfile.mkdtemp()

    try:

        domain_a_id = indexer.client.urlclient.get_domain_id("http://example-a.com/")
        domain_b_id = indexer.client.urlclient.get_domain_id("http://example-b.com/")
        domain_c_id = indexer.client.urlclient.get_domain_id("http://example-c.com/")

        sparksubmit("jobs/spark/index.py --source corpus:%s  --plugin plugins.linkgraph.DomainToDomain:coalesce=1,dir=%s/rdd/" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            linkgraph_dir
        ))

        # We collect(1) so there should be only one partition
        linkgraph_file = linkgraph_dir + "/rdd/part-00000"
        assert os.path.isfile(linkgraph_file)

        with open(linkgraph_file, "r") as f:
            graph = [x.split(" ") for x in f.read().strip().split("\n")]

        print graph
        assert len(graph) == 3
        assert [str(domain_a_id), str(domain_b_id)] in graph
        assert [str(domain_b_id), str(domain_c_id)] in graph
        assert [str(domain_c_id), str(domain_b_id)] in graph

    finally:
        shutil.rmtree(linkgraph_dir)
