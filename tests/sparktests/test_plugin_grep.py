import pytest
from test_index import CORPUSES
import tempfile
import shutil
import pipes
import os
import ujson as json


@pytest.mark.elasticsearch
def test_spark_plugin_grep(sparksubmit):

    tmp_dir = tempfile.mkdtemp()

    try:

        sparksubmit("spark/jobs/index.py --source corpus:%s --plugin plugins.filter.All:index=0 --plugin 'plugins.grep.Words:words=c1 d1 world,path=%s/out/,coalesce=1'" % (
            pipes.quote(json.dumps(CORPUSES["simple_link_graph_domain"])),
            tmp_dir
        ))

        parts = [os.path.join(tmp_dir, "out", f) for f in os.listdir(tmp_dir + "/out/") if f.startswith("part-")]
        assert len(parts) == 1
        with open(parts[0], "r") as f:
            data = set(f.read().strip().split("\n"))
            assert data == set([
                "d1,world http://example-d.com/page1",
                "c1 http://example-c.com/page1"
            ])

    finally:
        shutil.rmtree(tmp_dir)
