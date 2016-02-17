from .document.html import HTMLDocument
from .ranker import Ranker
from .urlclient import URLClient
from .es import ElasticsearchBulkIndexer
from .config import config
from .signals import load_signal
from .formatting import format_title, format_summary


class Indexer(object):
    """ Main glue class that manages the indexation of document instances into Elasticsearch """

    def __init__(self):
        self.es_docs = ElasticsearchBulkIndexer("docs")
        self.es_text = ElasticsearchBulkIndexer("text")
        self.urlclient = URLClient()
        self.ranker = Ranker(self.urlclient)
        self.lang_detector = load_signal("language").get_value

        self.connect()

    def connect(self):
        self.urlclient.connect()
        self.ranker.connect()

    def flush(self):
        self.es_docs.flush()
        self.es_text.flush()

    def refresh(self):
        self.es_docs.refresh()
        self.es_text.refresh()

    def empty(self):
        """ Resets all data. Will not run in production. """

        if config["ENV"] not in ("local", "ci"):
            raise Exception("Empty() not allowed in env %s" % config["ENV"])

        self.urlclient.empty()
        self.ranker.empty()
        self.es_docs.create(empty=True)
        self.es_text.create(empty=True)

    def index_documents(self, docs, flush=False, refresh=False):
        """ Index a group of documents """

        res = [
            self.index_document(doc["content"], url=doc.get("url"))
            for doc in docs
        ]

        if flush:
            self.flush()

        if refresh:
            self.refresh()

        return res

    def index_document(self, content, headers=None, url=None, links=False):
        """ Index one single document """

        doc = HTMLDocument(content, url=url, headers=headers)
        doc.parse()

        # Detect the language of the document
        langs = self.lang_detector(doc, None)

        # Free memory ASAP - we don't need raw data from now on
        del doc.source_data
        del doc.parser

        # Guess the main document URL
        main_url = doc.get_url()

        # Get metadata & ranks from the URLServer
        url_metadata = self.urlclient.get_metadata([main_url])[0]

        # Extract basic content
        url_words = doc.get_url_words()[0:50]
        domain_words = doc.get_domain_paid_words()[0:10]

        title = format_title(doc, url_metadata)
        summary = format_summary(doc, url_metadata)

        docid = url_metadata["url_id"]

        # Compute global rank
        rank, _ = self.ranker.get_global_document_rank(doc, url_metadata)

        # Insert in Document store
        es_doc = {
            "url": main_url.url,
            "title": title,
            "summary": summary
        }
        self.es_docs.index(docid, es_doc)

        # Insert in text index
        es_text = {
            "domain_words": domain_words,
            "url_words": url_words,
            "title": title,
            "rank": rank
        }

        for lang in langs:
            es_text["lang_%s" % lang] = langs[lang]

        # print es_text

        # Assemble the extracted word groups
        # TODO weights!
        word_groups = doc.get_word_groups()
        es_text["body"] = u" ".join([wg["words"].decode("utf-8", "ignore") for wg in word_groups])

        self.es_text.index(docid, es_text)

        # Return structured data for Spark operations that may happen after this
        ret = {
            "docid": docid,
            "url": main_url,
            "rank": rank
        }
        if links:
            ret["links"] = doc.get_hyperlinks()

        return ret
