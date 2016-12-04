# coding: utf-8

import tempfile
import os
import pipes
import pytest
import shutil
import ujson as json


CORPUS = {
	"desc": "Simple test with to backlinks to one page",
	"docs": [
		{
            "url": "http://example-a.com/page1",
			"content": """<html><title>Page A1</title><a href="http://example-c.com">First backlink</a><a href="http://example-c.com">Second backlink</a></html>"""
		},
		{
			"url": "http://example-c.com",
			"content": """<html><title>Example C</title></html>"""
		}
	]
}


def test_spark_plugin_backlinks(sparksubmit):
    tmp_dir = tempfile.mkdtemp()
    try:
        sparksubmit("spark/jobs/pipeline.py\
                     --source corpus:{corpus}\
                     --plugin plugins.backlinks.MostExternallyLinkedPages:domain=example-c.com,output={tmpdir}/out\
                     ".format(corpus=pipes.quote(json.dumps(CORPUS)),
                              tmpdir=tmp_dir))
        parts = [os.path.join(tmp_dir, 'out', f) for f in os.listdir(tmp_dir + '/out/') if f.startswith("part-")]
        assert len(parts) == 1
        with open(parts[0], 'r') as f:
            data = set(f.read().strip().split('\n'))
            assert data == set(["example-c.com/ 1 http://example-a.com/page1"])
    finally:
        shutil.rmtree(tmp_dir)
