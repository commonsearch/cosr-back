# pylint: disable=wrong-import-order
from gevent import monkey
monkey.patch_all()

from flask import Flask, request, render_template

import os
import sys
import json
import requests
from werkzeug.serving import run_simple

sys.path.insert(0, os.getcwd())

from cosrlib.document import load_document_type
from cosrlib.config import config
from cosrlib.searcher import Searcher
from cosrlib.indexer import Indexer


CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

app = Flask(
    "explainer",
    static_folder=os.path.join(CURRENT_DIRECTORY, "static"),
    template_folder=os.path.join(CURRENT_DIRECTORY, "templates")
)

indexer = Indexer()
indexer.connect()

searcher = Searcher()
searcher.connect()


@app.route('/')
def route_search():
    """ Homepage, for debugging searches """
    return render_template("search.html", config={})


@app.route('/url')
def route_url():
    """ URL page, for debugging parsing """
    return render_template("url.html", config={})


@app.route('/api/searchdebug')
def debug_search():
    """ API route for search debug """

    query = request.args.get("q")
    lang = request.args.get("g") or "en"

    results = searcher.search(query, lang=lang, explain=True, fetch_docs=True)

    return json.dumps(results)


@app.route('/api/urldebug')
def debug_url():
    """ API route for URL debug """

    # TODO: have a quota per ip on this API to prevent abuse

    url = request.args.get("url")

    # Special case for local files
    if url.startswith("tests/") and config["ENV"] == "local":
        with open(url, "rb") as f:
            cnt = f.read()
            headers = {}
    else:

        if not url.startswith("http"):
            url = "http://" + url

        req = requests.get(url)
        cnt = req.content
        headers = dict(req.headers)

    doc = load_document_type("html", cnt, url=str(url), headers=headers)

    parsed = indexer.parse_document(doc)

    global_rank, ranking_signals = indexer.ranker.get_global_document_rank(doc, parsed["url_metadata"])

    # URL class is not serializable
    links = [{
        "href": link["href"].url,
        "words": link.get("words")
    } for link in doc.get_hyperlinks()]

    ret = {
        "url": parsed["url"].url,
        "word_groups": doc.get_word_groups(),
        "rank": global_rank,
        "title_raw": doc.get_title(),
        "title": parsed["title_formatted"],
        "summary": parsed["summary_formatted"],
        "langs": parsed["langs"],
        "links": links,
        "ranking_signals": ranking_signals
    }

    return json.dumps(ret)


def main():
    app.debug = True
    print "Explainer listening on http://%s" % config["EXPLAINER"]
    run_simple(config["EXPLAINER"].split(":")[0], int(config["EXPLAINER"].split(":")[1]), app)


if __name__ == '__main__':
    main()
