import os
import tempfile
import pytest
import shutil
import ujson as json
import pipes


CORPUSES = {

    "simple_docs": {

        "desc": "Performs simple indexing tests",
        "docs": [{
            "url": "http://example.com/my-single-doc",
            "content": """<html><title>Hello</title> Hello world</html>"""
        }, {
            "url": "http://example.com/my-single-doc-2",
            "content": """<html><title>Single doc 2</title> Brave new world</html>"""
        }]
    },

    "simple_link_graph_domain": {
        "desc": "Simple link graph tests at the domain level.",

        # A -> B, A -> A, B -> C, C -> B
        "docs": [{
            "url": "http://example-a.com/page1",
            "content": """<html><title>Page A1</title> <a href="http://example-b.com/">Example B</a></html>"""
        }, {
            "url": "http://example-a.com/page2",
            "content": """<html><title>Page A2</title> <a href="/page1">A</a> <a href="http://example-b.com/">Example B</a> </html>"""
        }, {
            "url": "http://example-b.com/page1",
            "content": """<html><title>Page B1</title> <a href="http://example-c.com/">Example C</a></html>"""
        }, {
            "url": "http://example-c.com/page1",
            "content": """<html><title>Page C1</title> <a href="http://example-b.com/">Example B</a></html>"""
        }, {
            "url": "http://example-d.com/page1",
            "content": """<html><title>Page D1</title> Brave new world</html>"""
        }]

    }
}


@pytest.mark.elasticsearch
def test_spark_index(searcher, indexer, sparksubmit):

    # TODO: we could make this faster by disabling the web UI and other components?
    sparksubmit("spark/jobs/index.py --source corpus:%s" % pipes.quote(json.dumps(CORPUSES["simple_docs"])))

    search_res = searcher.client.search("hello", explain=False, lang=None, fetch_docs=True)
    hits = search_res["hits"]
    assert len(hits) == 1
    assert hits[0]["title"] == "Hello"
    assert hits[0]["url"] == "http://example.com/my-single-doc"

