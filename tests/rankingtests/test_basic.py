import pytest
import json

CORPUSES = [

    {
        "id": "single_doc",
        "desc": "Performs the most simple tests on a single document",
        "docs": [{
            "url": "http://example.com/my-single-doc",
            "content": """<html><title>Hello</title> Hello world</html>"""
        }],
        "searches": {
            None: {
                "unrelated query": [],
                "title": [],
                "html": [],
                "http": [],
                "hello": [0],
                "hello world": [0],
                "single doc world": [0],
                "single": [0],
                "example": [0]
            }
        }
    },

    {
        "id": "single_domain_docs",
        "desc": "Basic tests on documents from a single domain",
        "docs": [{
            "url": "http://example.com/",
            "content": """<html><title>Hello</title> Hello, I'm the homepage</html>"""
        }, {
            "url": "http://example.com/my-single-doc",
            "content": """<html><title>Hello</title> Hello world</html>"""
        }, {
            "url": "http://example.com/my-single-doc-2",
            "content": """<html><title>Hello</title> Hello world, second.</html>"""
        }, {
            "url": "http://example.com/my-single-doc-33",
            "content": """<html><title>Hello second</title> Hello world.</html>"""
        }],
        "explain": False,
        "searches": {
            None: {

                "unrelated query": [],

                # Contained in everyone, so this should be ranked primarily by URL length and density in titles.
                "hello": [0, 1, 2, 3],
                "example": [0, 1, 2, 3],

                # words are more important in titles
                "second": [3, 2],
                "homepage": [0],

                # Exact phrase vs. AND
                # '"hello second"': [3],
                # 'hello second': [3, 2],
            }
        }
    },
    {
        "id": "homepage_domain_vs_title",
        "desc": "Homepage domain relevancy vs. title relevancy",
        "docs": [{
            "url": "http://example.com/",
            "content": """<html><title>Large title for the homepage</title> Homepage text</html>"""
        }, {
            "url": "http://example.com/subpage",
            "content": """<html><title>Example</title> Subpage text</html>"""
        }, {
            "url": "http://otherdomain.com/example",
            "content": """<html><title>Title</title> Subpage text</html>"""
        }],
        "explain": False,
        "searches": {
            None: {
                "example": [1, 0, 2]
            }
        }
    },
    {
        "id": "www_preferred",
        "desc": "Prefer www to other shorted subdomains",
        "docs": [{
            "url": "http://www.example.com/",
            "content": """<html><title>Title</title> Homepage text</html>"""
        }, {
            "url": "http://ww.example.com/",
            "content": """<html><title>Title</title> Homepage text</html>"""
        }, {
            "url": "http://w.example.com/",
            "content": """<html><title>Title</title> Homepage text</html>"""
        }],
        "explain": True,
        "searches": {
            None: {
                "example": [0, 2, 1]
            }
        }
    },
    {
        "id": "language_selection",
        "desc": "Language settings have an influence on results",
        "docs": [{
            "url": "http://example.com/newspaper",
            "content": """<html><title>The Newspaper Le Monde</title> Le Monde is a French daily evening newspaper founded by Hubert Beuve-Mery and continuously published in Paris since its first edition on 19 December 1944. It is one of the most important and widely respected newspapers in the world.</html>"""
        }, {

            # This will be detected as french
            "url": "http://example.com/",
            "content": """<html><title>Not even in title!</title> Le Monde est l'un des derniers quotidiens francais dits du soir, qui parait a Paris en debut d'apres-midi. Il est aussi disponible dans une version en ligne.</html>"""
        }],
        "explain": False,
        "searches": {
            "fr": {
                "monde": [1, 0],
                "newspaper": [0]
            },
            "en": {
                "monde": [0, 1],
                "derniers": [1]
            },
            None: {
                "monde": [0, 1],
                "newspaper": [0]
            }
        }
    },
    {
        "id": "multiple_terms_simple",
        "desc": "Test relevance with multiple terms",
        "docs": [{
            "url": "http://example.com/1",
            "content": """<html><title>Example</title> This is a great example of text.</html>"""
        }, {
            "url": "http://example.com/2",
            "content": """<html><title>Example text</title> This is a great example.</html>"""
        }, {
            "url": "http://example.com/text",
            "content": """<html><title>Example text</title> This is a great example.</html>"""
        }, {
            "url": "http://text.example.com/",
            "content": """<html><title>Example text</title> This is a great example.</html>"""
        }, {
            "url": "http://a-larger-example-url.com/",
            "content": """<html><title>No good title</title> This is a great example text.</html>"""
        }],
        "explain": False,
        "searches": {
            None: {
                "example": [0, 1, 3, 2, 4],
                "example text": [3, 2, 1, 0, 4],
                "text example": [3, 2, 1, 0, 4],
                "text": [3, 2, 1, 0, 4]
            }
        }
    },
    {
        "id": "term_frequencies_simple",
        "desc": "Test the impact of term frequencies on different fields",
        "docs": [{
            "url": "http://example.com/0-example-abcdefg",
            "content": """<html><title>Example title</title> This is a great example of text.</html>"""
        },
        {
            "url": "http://example.com/1-example-abcdefg",
            "content": """<html><title>Example title</title> This is a great example of text. Example - ary!</html>"""
        },
        {
            "url": "http://example.com/2-example-example-abcdefg-abcdefg",
            "content": """<html><title>Example title</title> This is a great example of text. Example - ary!</html>"""
        },
        {
            "url": "http://example.com/3-example-example-example-example",
            "content": """<html><title>Example example title</title> This is a great abcdefg of text. abcdefg - ary!</html>"""
        },
        {
            "url": "http://example.com/4-example-abcdefg-abcdefg-abcdefg",
            "content": """<html><title>Title</title> This is a great example of text. example - ary! example example example example example example example example example</html>"""
        }
        ],
        "explain": False,
        "searches": {
            None: {
                "example": [1, 0, 2, 3, 4]
            }
        }
    },
    {
        "id": "test_popularity_rank",
        "desc": "Test the impact of the popularity on the ranks",
        "docs": [{
            "url": "http://abcde-example.com/0",
            "content": """<html><title>Example</title> This is a great example of text with title.</html>""",
            "url_metadata_extra": {
                "alexa_top1m_rank": 1
            }
        },
        {
            "url": "http://abcde-example.com/1",
            "content": """<html><title>Example title</title> This is a great example of text.</html>""",
            "url_metadata_extra": {
                "alexa_top1m_rank": 100
            }
        },
        {
            "url": "http://abcde-example.com/2",
            "content": """<html><title>Example title</title> This is a great example of text.</html>""",
            "url_metadata_extra": {
                "alexa_top1m_rank": 10000
            }
        },
        {
            "url": "http://abcde-example.com/3",
            "content": """<html><title>Example title</title> This is a great example of text.</html>""",
            "url_metadata_extra": {
                "alexa_top1m_rank": 100000
            }
        },
        {
            "url": "http://abcde-example.com/4",
            "content": """<html><title>Example</title> This is a great example of text with nothing more to say.</html>""",
            "url_metadata_extra": {
                "alexa_top1m_rank": 1
            }
        }],
        "explain": False,
        "searches": {
            None: {
                "example": [0, 4, 1, 2, 3],
                "example title": [1, 2, 0, 3, 4]
            }
        }
    },
    {
        "id": "test_partial_url_match",
        "desc": "Test partial matching on URLs and domains",
        "docs": [{
            "url": "http://www.example.com/0-page-link",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://text.example.com/1-page-link",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://textexample.com/2-abcd-efgh",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://mytextexample.com/3-abcd-efgh",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://mytextnexample.com/4-abcd-efgh",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://otherdomain.com/5-text-example.html",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://otherdomain.com/6-textexample.html",
            "content": """<html><title>Example Text!</title> Body</html>"""
        },
        {
            "url": "http://textexample.com/7-abcd-efgf",
            "content": """<html><title>Other title</title> Body</html>"""
        }],
        "explain": False,
        "searches": {
            None: {
                "example": [0, 1, 2, 3, 4, 5, 6],
                "example text": [2, 3, 4, 1, 0, 5, 6]
            }
        }
    }
]


@pytest.mark.elasticsearch
@pytest.mark.parametrize(("corpus_id", "corpus_index"), [(c["id"], i) for i, c in enumerate(CORPUSES)])
def test_ranking_corpuses(corpus_id, corpus_index, indexer, searcher):
    corpus = CORPUSES[corpus_index]

    index_res = indexer.client.index_documents(corpus["docs"], flush=True, refresh=True)

    docids_map = {
        str(r["docid"]): i for i, r in enumerate(index_res)
    }

    for lang, searches in corpus["searches"].iteritems():
        for search, expected_indexes in searches.iteritems():
            search_res = searcher.client.search(search, explain=bool(corpus.get("explain")), lang=lang)
            hits = search_res["hits"]

            hits_indexes = [docids_map[str(hit["docid"])] for hit in hits]

            print "*" * 50
            print "SEARCH [lang=%s]: %s" % (lang, search)
            print
            for hit in hits:
                doc_index = docids_map[str(hit["docid"])]
                print " Doc #%s (%s)  %s" % (doc_index, corpus["docs"][doc_index]["url"], corpus["docs"][doc_index]["content"][0:50])
                print "   rank:   %s" % hit["rank"]

                # https://github.com/elastic/elasticsearch/issues/15369
                print "   score:  %s" % hit["score"]
                if corpus.get("explain"):
                    print "   expln:  %s" % (hit["explain"])

            print

            assert (expected_indexes == hits_indexes), search
