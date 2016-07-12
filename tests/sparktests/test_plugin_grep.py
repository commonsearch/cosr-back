import pytest
import os
from test_index import CORPUSES
import tempfile
import shutil
import pipes
import ujson as json


@pytest.mark.elasticsearch
def test_spark_plugin_grep(searcher, indexer):

    tmp_dir = tempfile.mkdtemp()

    try:
        os.system("spark-submit jobs/spark/index.py --source corpus:%s  --plugin 'plugins.grep.Words:words=c1 d1 world,dir=%s/rdd/,coalesce=1'" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            tmp_dir
        ))

        with open(tmp_dir + "/rdd/part-00000", "r") as f:
            data = set(f.read().strip().split("\n"))
            assert data == set([
                "d1,world http://example-d.com/page1",
                "c1 http://example-c.com/page1"
            ])

    finally:
        shutil.rmtree(tmp_dir)
