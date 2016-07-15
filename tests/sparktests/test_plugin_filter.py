import pytest
import os
from test_index import CORPUSES
import tempfile
import shutil
import pipes
import ujson as json

content = """
    <title>testindex</title>
    %s
    <p>testbody</p>
""" % ("<p>aaaaaaaaa </p> " * 100)

CORPUS = {
    "docs": [
        {
            "url": "http://www.example.com/",
            "content": content
        },
        {
            "url": "http://www.example.com/page1",
            "content": content
        },
        {
            "url": "http://example2.com/page2",
            "content": content
        },
        {
            "url": "http://example3.com/",
            "content": content
        }
    ]
}


@pytest.mark.elasticsearch
def test_spark_plugin_filter(searcher, indexer):

    # Filters are taken in order.
    # With this, we disable parsing (hence indexation) for all documents by defaults,
    # and then selectively re-enable features with some filters
    os.system(
        "spark-submit jobs/spark/index.py " +
        " --plugin plugins.filter.All:parse=0 " +
        " --plugin plugins.filter.Homepages:index_body=0 " +
        " --plugin plugins.filter.Domains:domains=example2.com,index_body=1 " +
        " --plugin plugins.filter.Domains:domains=example3.com,index=0 " +
        " --source corpus:%s " % (
            pipes.quote(json.dumps(CORPUS))
        )
    )

    search_res = searcher.client.search("testbody", explain=False, lang=None, fetch_docs=True)
    assert len(search_res["hits"]) == 1
    assert search_res["hits"][0]["url"] == "http://example2.com/page2"

    search_res = searcher.client.search("testindex", explain=False, lang=None, fetch_docs=True)
    assert len(search_res["hits"]) == 2
    assert set([
        search_res["hits"][0]["url"], search_res["hits"][1]["url"]
    ]) == set([
        "http://example2.com/page2", "http://www.example.com/"
    ])
