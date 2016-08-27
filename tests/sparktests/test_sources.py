import pytest
import shutil
import tempfile
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
    ],
    "block": "1"
}


@pytest.mark.elasticsearch
def test_source_multiple(searcher, indexer, sparksubmit):

    # Sources are done in order and overlapping documents are overwritten
    # This is because they both use block=1
    sparksubmit(
        """spark/jobs/pipeline.py \
         --plugin plugins.filter.All:index_body=1 \
         --source wikidata:block=1 \
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


def test_source_commoncrawl(sparksubmit):

    tmp_dir = tempfile.mkdtemp()

    try:

        sparksubmit(
            """spark/jobs/pipeline.py \
            --source commoncrawl:limit=2,maxdocs=3 \
            --plugin plugins.dump.DocumentMetadata:format=json,path=%s/intermediate/ """ % (
                tmp_dir
            )
        )

        intermediate_dir = os.path.join(tmp_dir, "intermediate")
        files = [f for f in os.listdir(intermediate_dir) if f.endswith(".json")]
        assert len(files) == 1

        items = []
        with open(os.path.join(intermediate_dir, files[0]), "r") as jsonf:
            for line in jsonf.readlines():
                items.append(json.loads(line.strip()))

        # print items

        assert len(items) == 6

        assert "id" in items[0]
        assert "url" in items[0]

        # This is a silly test but it should work: read the metadata and dump it again somewhere else,
        # but this time as parquet!

        sparksubmit(
            """spark/jobs/pipeline.py \
            --source metadata:format=json,path=%s/intermediate/ \
            --plugin plugins.dump.DocumentMetadata:format=parquet,path=%s/intermediate2/ """ % (
                tmp_dir, tmp_dir
            )
        )

        from .test_plugin_webgraph import _read_parquet

        data = _read_parquet(os.path.join(tmp_dir, "intermediate2"))

        assert len(data) == 6
        assert "url" in data[0]

    finally:
        shutil.rmtree(tmp_dir)
