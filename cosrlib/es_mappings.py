from __future__ import absolute_import, division, print_function, unicode_literals

# https://www.elastic.co/guide/en/elasticsearch/guide/current/changing-similarities.html
ES_SIMILARITIES = {
    "title_bm25": {
        "type": "BM25",
        "b": 0.75,
        "k1": 1
    }
}


ES_MAPPINGS = {

    # Text index: contains the main fulltext inverted index
    "text": {

        "page": {

            # No need to store the source document
            "_source": {"enabled": False},
            "_all": {"enabled": False},


            # We use a dynamic template because there are lots of lang_XX fields
            "dynamic_templates": [
                {
                    "url_words": {
                        "match": "url_words",
                        "mapping": {
                            "type": "string",
                            "store": False,
                            "doc_values": False,
                            "similarity": "title_bm25"
                        }
                    }
                },
                {
                    "domain_words": {
                        "match": "domain_words",
                        "mapping": {
                            "type": "string",
                            "store": False,
                            "doc_values": False,
                            "similarity": "title_bm25"
                        }
                    }
                },
                {
                    "paid_domain_words": {
                        "match": "paid_domain_words",
                        "mapping": {
                            "type": "string",
                            "store": False,
                            "doc_values": False,
                            "similarity": "title_bm25"
                        }
                    }
                },
                {
                    "domain": {
                        "match": "domain",
                        "mapping": {
                            "type": "string",
                            "index":"not_analyzed",
                            "store": True,
                            "doc_values": True  # Will we actually need to aggregate on the domain later?
                        }
                    }
                },
                {
                    "title": {
                        "match": "title",
                        "mapping": {
                            "type": "string",
                            "store": False,
                            "doc_values": False,
                            "similarity": "title_bm25"
                        }
                    }
                },
                {
                    "summary": {
                        "match": "summary",
                        "mapping": {
                            "type": "string",
                            "store": False,
                            "doc_values": False,
                            "similarity": "title_bm25"
                        }
                    }
                },
                {
                    "body": {
                        "match": "body",
                        "mapping": {
                            "type": "string",
                            "store": False,
                            "doc_values": False
                        }
                    }
                },
                {
                    "rank": {
                        "match": "rank",
                        "mapping": {
                            "type": "float",
                            "store": True,
                            "doc_values": True
                        }
                    }
                },
                {
                    "langs": {
                        "match": "lang_*",
                        "mapping": {
                            "type": "float",
                            "store": True,
                            "doc_values": True
                        }
                    }
                }
            ]
        }
    },

    # Docs index: contains the document metadata neede to display search results
    # This is mainly used as a key-value document store.
    "docs": {

        "page": {

            "_source": {"enabled": False},
            "_all": {"enabled": False},

            "properties": {

                "url": {
                    "type": "string",
                    "store": True,
                    "index": "no",
                    "doc_values": False
                },
                "title": {
                    "type": "string",
                    "store": True,
                    "index": "no",
                    "doc_values": False
                },
                "summary": {
                    "type": "string",
                    "store": True,
                    "index": "no",
                    "doc_values": False
                },
                # "body": {
                #     "type": "string",
                #     "store": False
                # },
            }
        }
    }
}
