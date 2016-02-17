import os
import tempfile
import pytest
import shutil


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
def test_spark_index(searcher, indexer):
    from cosrlib.webarchive import create_warc_from_corpus
    warc_file = create_warc_from_corpus(CORPUSES["simple_docs"]["docs"])

    try:

        # TODO: we could make this faster by disabling the web UI and other components?
        os.system("spark-submit jobs/spark/index.py --warc_files %s" % warc_file)

        search_res = searcher.client.search("hello", explain=False, lang=None, fetch_docs=True)
        hits = search_res["hits"]
        assert len(hits) == 1
        assert hits[0]["title"] == "Hello"
        assert hits[0]["url"] == "http://example.com/my-single-doc"

    finally:
        os.remove(warc_file)


@pytest.mark.elasticsearch
def test_spark_link_graph(searcher, indexer):
    from cosrlib.webarchive import create_warc_from_corpus
    warc_file = create_warc_from_corpus(CORPUSES["simple_link_graph_domain"]["docs"])

    linkgraph_dir = tempfile.mkdtemp()

    try:

        domain_a_id = indexer.client.urlclient.get_domain_id("http://example-a.com/")
        domain_b_id = indexer.client.urlclient.get_domain_id("http://example-b.com/")
        domain_c_id = indexer.client.urlclient.get_domain_id("http://example-c.com/")

        os.system("spark-submit jobs/spark/index.py --warc_files %s --save_linkgraph_domains %s/rdd/" % (
            warc_file, linkgraph_dir
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
        os.remove(warc_file)
        shutil.rmtree(linkgraph_dir)
