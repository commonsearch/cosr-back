from elasticsearch import Elasticsearch

from .config import config


def format_explain(explain, level=0):
    buf = "%s%s | %s\n" % ("    " * level, explain["value"], explain["description"])
    for child in explain.get("details", []):
        buf += format_explain(child, level + 1)
    return buf


class Searcher(object):
    """ Search client. This duplicates some of the Go code from cosr-front,
        mainly for testing purposes """

    def __init__(self):
        self.es_text = Elasticsearch([config["ELASTICSEARCHTEXT"]])
        self.es_docs = Elasticsearch([config["ELASTICSEARCHDOCS"]])

    def connect(self):
        pass

    def empty(self):
        pass

    def search(self, q, explain=False, lang=None, fetch_docs=False):
        """ Performs a search on Common Search. Only for tests """

        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html
        scoring_functions = [
            {
                "field_value_factor": {
                    "field": "rank",
                    "missing": 0
                }
            }
        ]

        if lang is not None:
            scoring_functions.append({
                "field_value_factor": {
                    "field": "lang_%s" % lang,
                    "missing": 0.1
                }
            })

        # https://www.elastic.co/guide/en/elasticsearch/guide/current/boosting-by-popularity.html
        es_body = {
            "query": {
                "function_score": {
                    "query": {
                        "multi_match": {
                            "query": q,
                            "fields": ["title", "body", "url_words", "domain_words^2"]
                        }
                    },

                    # Defaults
                    # "score_mode": "multiply",
                    # "boost_mode": "multiply",
                    "functions": scoring_functions,

                }
            },
            "explain": explain
        }
        # print es_body

        es_text_result = self.es_text.search(  # pylint: disable=unexpected-keyword-arg
            index="text",
            doc_type="page",
            body=es_body,
            fields="*",
            sort="_score:desc",
            from_=0,
            search_type="dfs_query_then_fetch",
            size=50
        )
        # print es_text_result

        res = {"hits": []}
        for hit in es_text_result["hits"]["hits"]:
            row = {
                "score": hit["_score"],
                "docid": int(hit["_id"])
            }
            if "rank" in hit.get("fields", {}):
                row["rank"] = hit["fields"]["rank"][0]
            if explain:
                row["explain"] = format_explain(hit.get("_explanation"))
            res["hits"].append(row)

        if fetch_docs and len(es_text_result["hits"]["hits"]) > 0:

            doc_indexes = {x["_id"]: i for i, x in enumerate(es_text_result["hits"]["hits"])}

            es_docs_result = self.es_docs.search(  # pylint: disable=unexpected-keyword-arg
                index="docs",
                doc_type="page",
                body={"query": {"filtered": {"filter": {"ids": {
                    "type": "page",
                    "values": [x["_id"] for x in es_text_result["hits"]["hits"]]
                }}}}},
                fields="*",
                from_=0,
                size=len(es_text_result["hits"]["hits"])
            )

            for hit in es_docs_result["hits"]["hits"]:
                idx = doc_indexes[hit["_id"]]
                for f in hit["fields"]:
                    res["hits"][idx][f] = hit["fields"][f][0]

        return res
