import importlib
import os

from cosrlib import re
from cosrlib.url import URL


_RE_SPLIT_URLWORDS = re.compile(r"[^a-z0-9]+")
_RE_SPLIT_WORDS = re.compile(r"[\s\W]+")
_RE_WHITESPLACE = re.compile(r"[\s]+")
_RE_STRIP_PROTOCOL = re.compile(r"^.*\/\/")


def load_document_type(doctype, *args, **kwargs):
    """ Loads and instanciates a [HTML, ...]Document class from a doctype """
    cls_name = "%sDocument" % doctype.upper()
    cls = getattr(importlib.import_module("cosrlib.document.%s" % doctype), cls_name)
    return cls(*args, **kwargs)


class Document(object):
    """ An indexable document. Base class for all document types (HTML, PDF, ...) """

    def __init__(self, source_data, url=None, headers=None, index_level=2):
        self.source_data = source_data
        self.source_headers = headers or {}
        self.index_level = index_level

        if not url:
            self.source_url = URL("")
        elif isinstance(url, basestring):
            self.source_url = URL(url)
        else:
            self.source_url = url

        self._word_groups = []

    def parse(self):
        """ CPU-bound parsing of source_data into useful data.

            This method should delete the source_data afterwards to free memory ASAP so it can
            only be called once.
        """

        return self

    def discard_source_data(self):
        """ Remove source_data from memory """
        return

    def get_title(self):
        """ Returns document title, without any cleaning """
        return None

    def get_all_words(self):
        """ Returns a set with all the words in the document. Mostly used for simple tests """
        words = set()

        if self.get_title():
            words |= set(_RE_SPLIT_WORDS.split(self.get_title().lower()))

        for g in self._word_groups:
            words |= set(_RE_SPLIT_WORDS.split(g["words"].lower()))

        return words

    def get_word_groups(self):
        """ Returns a dict of groups of words appearing in the document,
            with weights and other metadata """

        return self._word_groups

    def add_word_group(self, words, weight=1, **kwargs):
        """ Add a word group """

        if not words:
            return

        wg = {
            "words": words,
            "weight": weight
        }
        if kwargs:
            wg.update(kwargs)

        self._word_groups.append(wg)

    def get_path_words(self):
        """ Returns a list of words found in the URL path """
        url = self.get_url()
        if not url:
            return []

        return self._split_url_words(url.parsed.path)

    def get_domain_words(self, with_paid_domain=True):
        """ Returns a list of words found in the domain """

        if not with_paid_domain:
            return self._split_url_words(self.source_url.normalized_subdomain)
        else:
            return self._split_url_words(self.source_url.normalized_domain)

    def get_domain_paid_words(self):
        """ Returns a list of words found in the paid-level domain """
        url = self.source_url.tldextracted.domain
        return self._split_url_words(url)

    def get_url_words(self):
        """ Returns a list of words found in the URL """
        return self.get_domain_words() + self.get_path_words()

    def get_hyperlinks(self):
        """ Returns a list of followable URLs found in the document """
        return []

    def get_external_hyperlinks(self):
        """ Returns a list of followable URLs found in the document """
        return []

    def get_internal_hyperlinks(self):
        """ Returns a list of followable URLs found in the document """
        return []

    def get_head_metas(self):
        """ Returns metadata from the document header """
        return {}

    # TODO: validate it's from the same domain?
    def parse_canonical_url(self):
        """ Extract a canonical URL from the document """
        return None

    def get_url(self):
        """ Returns our best guess for the unique, canonical URL for this document. """
        return self.parse_canonical_url() or self.source_url

    def get_urls(self):
        """ Returns all potential canonical URLs for this document """
        urls = [self.source_url]
        canonical = self.parse_canonical_url()
        if canonical and canonical.url != self.source_url.url:
            urls.append(canonical)
        return urls

    def _split_words(self, text):
        """ Simple word tokenization from text. """
        # TODO, make this into a proper tokenizer!
        return [x for x in _RE_WHITESPLACE.split(text) if x]

    def _split_url_words(self, url):
        """ Simple word tokenization from URLs. """
        if url is None:
            return []
        return [x for x in _RE_SPLIT_URLWORDS.split(_RE_STRIP_PROTOCOL.sub("", url.lower())) if x]

    def _split_filename_words(self, url):
        """ Simple word tokenization from filenames in URLs. """
        file_name = os.path.basename(os.path.splitext(url)[0])
        return self._split_url_words(file_name)
