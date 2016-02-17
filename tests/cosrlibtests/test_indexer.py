import pytest


@pytest.mark.elasticsearch
def test_simple_insert_and_search(indexer, searcher):
    from cosrlib.url import URL

    indexed = indexer.client.index_document(
      """<html><title>hello world</title><body>hello body</body></html>""",
      url=URL("http://example.com")
    )

    indexed2 = indexer.client.index_document(
      """<html><title>another world</title><body>document body</body></html>""",
      url=URL("http://example.com/page2")
    )
    indexer.client.flush()
    indexer.client.refresh()

    assert indexed["docid"]
    assert indexed["url"].url == "http://example.com"
    assert indexed["rank"] > 0

    assert indexed2["docid"] != indexed["docid"]

    search_results = searcher.client.search("hello")
    assert len(search_results["hits"]) == 1
    assert search_results["hits"][0]["docid"] == indexed["docid"]
    assert search_results["hits"][0]["score"] > 0

    search_results = searcher.client.search("world")
    assert len(search_results["hits"]) == 2
