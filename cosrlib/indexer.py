from .document.html import HTMLDocument
from .ranker import Ranker
from .urlclient import URLClient
from .es import ElasticsearchBulkIndexer
from .config import config
from .signals import load_signal
from .formatting import format_title, format_summary, infer_subwords


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

    def index_corpus(self, corpus, flush=False, refresh=False):
        """ Index a list of raw html documents. Mostly used in tests """

        res = [
            self.index_document(
                HTMLDocument(doc["content"], url=doc.get("url"), headers=doc.get("headers")),
                url_metadata_extra=doc.get("url_metadata_extra")
            )
            for doc in corpus
        ]

        if flush:
            self.flush()

        if refresh:
            self.refresh()

        return res

    def parse_document(self, doc, url_metadata_extra=None):
        """ Extract as much info as possible from a document for indexing """

        # Do the actual HTML parsing
        doc.parse()

        parsed = {

            # Detect the language of the document
            "langs": self.lang_detector(doc, None),

            # Guess the main document URL
            "url": doc.get_url(),

            # Splitted words in the URL
            "url_words": doc.get_path_words()[0:50],

            # Splitted words in the URL
            "domain_words": doc.get_domain_words()[-10:],

            # Splitted words in the paid domain
            "paid_domain_words": doc.get_domain_paid_words()[-10:]
        }

        # Get metadata from the URLServer
        parsed["url_metadata"] = self.urlclient.get_metadata([parsed["url"]])[0]

        # Used mostly in tests, to measure the influence one particular signal
        if url_metadata_extra:
            for k, v in url_metadata_extra.iteritems():
                for item_k, item_v in v.iteritems():
                    if hasattr(parsed["url_metadata"][k], item_k):
                        setattr(parsed["url_metadata"][k], item_k, item_v)

        # Format basic content
        parsed["title_formatted"] = format_title(doc, parsed["url_metadata"])
        parsed["summary_formatted"] = format_summary(doc, parsed["url_metadata"])

        # Infer words from concatenated strings ("lemonde" => "le monde")
        inferred_url_words = infer_subwords(
            parsed["url_words"],
            [parsed["title_formatted"], parsed["summary_formatted"]]
        )
        inferred_domain_words = infer_subwords(
            parsed["domain_words"],
            [parsed["title_formatted"], parsed["summary_formatted"]]
        )
        inferred_paid_domain_words = infer_subwords(
            parsed["paid_domain_words"],
            [parsed["title_formatted"], parsed["summary_formatted"]]
        )

        if parsed["url_words"] != inferred_url_words:
            parsed["url_words_inferred"] = inferred_url_words

        if parsed["domain_words"] != inferred_domain_words:
            parsed["domain_words_inferred"] = inferred_domain_words

        if parsed["paid_domain_words"] != inferred_paid_domain_words:
            parsed["paid_domain_words_inferred"] = inferred_paid_domain_words

        return parsed

    def index_document(self, doc, url_metadata_extra=None):
        """ Index one single document """

        parsed = self.parse_document(doc, url_metadata_extra=url_metadata_extra)

        docid = parsed["url_metadata"]["url"].id

        # Free memory ASAP - we don't need raw data from now on
        doc.discard_source_data()

        # Compute global rank
        if url_metadata_extra and "rank" in url_metadata_extra.get("url", {}):

            # Used mostly to bypass rank computation in tests
            rank = url_metadata_extra["url"]["rank"]

        else:
            rank, _ = self.ranker.get_global_document_rank(doc, parsed["url_metadata"])

        # Return structured data for Spark operations that may happen after this
        metadata = {
            "id": docid,
            "url": parsed["url"].url,
            "rank": rank
        }

        # We are not actually indexing this document, only return its metadata
        if doc.index_level == 0:
            return metadata

        # Insert in Document store
        es_doc = {
            "url": parsed["url"].url.decode("utf-8"),
            "title": parsed["title_formatted"],
            "summary": parsed["summary_formatted"]
        }

        self.es_docs.index(docid, es_doc)

        # Insert in text index
        es_text = {
            "domain_words": (
                [parsed["domain_words"], parsed["domain_words_inferred"]]
                if parsed.get("domain_words_inferred")
                else parsed["domain_words"]
            ),
            "paid_domain_words": (
                [parsed["paid_domain_words"], parsed["paid_domain_words_inferred"]]
                if parsed.get("paid_domain_words_inferred")
                else parsed["paid_domain_words"]
            ),
            "domain": parsed["url"].normalized_domain.decode("utf-8", "ignore"),
            "url_words": (
                [parsed["url_words"], parsed["url_words_inferred"]]
                if parsed.get("url_words_inferred")
                else parsed["url_words"]
            ),
            "title": parsed["title_formatted"],  # TODO: should we index the formatted version?
            "rank": rank
        }

        for lang, weight in parsed["langs"].iteritems():
            es_text["lang_%s" % lang] = weight

        # print es_text

        # Assemble the extracted word groups
        # TODO weights! https://github.com/commonsearch/cosr-back/issues/5
        if doc.index_level > 1:
            word_groups = doc.get_word_groups()
            es_text["body"] = u" ".join([wg["words"].decode("utf-8", "ignore") for wg in word_groups])

        self.es_text.index(docid, es_text)

        return metadata
