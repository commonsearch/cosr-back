import pytest
import os
import pipes
import ujson as json

CORPUS = {
    "docs": [
        {
            "url": "http://www.douglasadams.com/",
            "content": """ <title>xxxxuniquecontent</title> """
        },
        {
            "url": "http://www.example.com/page1",
            "content": """ <title>xxxxuniquecontent2</title> """
        }
    ]
}


@pytest.mark.elasticsearch
def test_spark_multiple_sources(searcher, indexer, sparksubmit):

    # Sources are done in order and overlapping documents are overwritten
    sparksubmit(
        """spark/jobs/index.py \
         --source wikidata \
         --source corpus:%s """ % (
            pipes.quote(json.dumps(CORPUS)),
        )
    )

    # From wikidata only
    search_res = searcher.client.search("sfgov", explain=False, lang=None, fetch_docs=True)
    assert len(search_res["hits"]) == 1
    assert search_res["hits"][0]["url"] == "http://sfgov.org"

    # From corpus only
    search_res = searcher.client.search("xxxxuniquecontent2", explain=False, lang=None, fetch_docs=True)
    assert len(search_res["hits"]) == 1
    assert search_res["hits"][0]["url"] == "http://www.example.com/page1"

    # Overwritten from corpus
    search_res = searcher.client.search("xxxxuniquecontent", explain=False, lang=None, fetch_docs=True)
    assert len(search_res["hits"]) == 1
    assert search_res["hits"][0]["url"] == "http://www.douglasadams.com/"

    search_res = searcher.client.search("douglasadams", explain=False, lang=None, fetch_docs=True)
    assert len(search_res["hits"]) == 1
    assert search_res["hits"][0]["url"] == "http://www.douglasadams.com/"
