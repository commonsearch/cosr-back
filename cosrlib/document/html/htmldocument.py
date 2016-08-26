import urlparse

from cosrlib import re
from cosrlib.url import URL
from cosrlib.document import Document

from .htmlencoding import HTMLEncoding
from .parsers import GUMBOCY_PARSER


_RE_STRIP_TAGS = re.compile(r"<.*?>")
_RE_VALID_NETLOC = re.compile(r"^[a-zA-Z0-9-]+\.[a-zA-Z0-9.:-]+$")


class HTMLDocument(Document):
    """ Class representing an HTML document, with methods for parsing and extracting content """

    # pylint: disable=attribute-defined-outside-init

    def __init__(self, *args, **kwargs):
        Document.__init__(self, *args, **kwargs)
        self.encoding = HTMLEncoding(self)
        self.analysis = None

    def discard_source_data(self):
        """ Remove source_data from memory """
        del self.source_data

    def parse(self):
        """
            Custom HTML parsing with tree traversal.

            This is based on gumbo, mainly because it was already battle-tested on a
            large subset of the web.

            For now we are trying to avoid using higher-level libraries like lxml because
            their performance may be a bottleneck at scale. Let's try to minimize operations
            on the source HTML as much as possible.

        """

        # Gumbo only accepts utf-8, we must convert if needed
        self.encoding.ensure_utf8()

        GUMBOCY_PARSER.parse(self.source_data)

        self.analysis = GUMBOCY_PARSER.analyze(url=self.source_url.url)

        for wg in (self.analysis.get("word_groups") or []):
            self.add_word_group(
                words=wg[0],
                tag=wg[1]
            )

        return self

    def get_internal_hyperlinks(self, exclude_nofollow=False):
        """ Returns a list of followable URLs to other domains found in the document """
        return [
            {
                "path": href,  # Unresolved raw path!
                "text": text.strip()
            }
            for href, text, rel in self.analysis["internal_hyperlinks"]
            if not (exclude_nofollow and rel == "nofollow")
        ]

    def get_external_hyperlinks(self, exclude_nofollow=False):
        """ Returns a list of followable URLs to other domains found in the document """
        return self._format_hyperlinks(
            self.analysis["external_hyperlinks"],
            exclude_nofollow=exclude_nofollow
        )

    def get_hyperlinks(self, exclude_nofollow=False):
        """ Returns a list of followable URLs found in the document """
        return self._format_hyperlinks(
            self.analysis["internal_hyperlinks"] +
            self.analysis["external_hyperlinks"],
            exclude_nofollow=exclude_nofollow
        )

    def _format_hyperlinks(self, links, exclude_nofollow=False):
        """ Formats a list of hyperlinks for return """

        if self.analysis.get("base_url"):
            base_url = URL(self.analysis["base_url"])
        else:
            base_url = self.source_url

        hyperlinks = []
        for href, text, rel in links:
            url = URL(base_url.urljoin(href), check_encoding=True)

            if (
                    # Probably a forgotten mailto:
                    "@" not in url.parsed.path and

                    # Probably an html error
                    not href.startswith("<") and

                    not (exclude_nofollow and rel == "nofollow") and

                    # This regex catches several things we don't want to follow:
                    # invalid hosts, TLDs, usernames, ..
                    _RE_VALID_NETLOC.match(url.parsed.netloc)
            ):
                hyperlinks.append({
                    "href": url,
                    "text": text.strip()
                })

        return hyperlinks

    def get_title(self):
        return self.analysis.get("title")

    def get_head_metas(self):
        """ Returns metadata from the document header """
        return self.analysis.get("head_metas") or {}

    def parse_canonical_url(self):
        """ Look for a valid meta tag rel="canonical" in the HEAD of the page.
            Returns an absolute URL in all cases """

        # TODO: should we use og:url as canonical url? http://ogp.me/

        meta_rel_canonical = [
            x for x in self.analysis.get("head_links", [])
            if (x.get("rel") or "").lower() == "canonical"
        ]
        if len(meta_rel_canonical) > 0:
            canonical_url = meta_rel_canonical[0].get("href") or None

            if not canonical_url:
                return None

            canonical_url = URL(canonical_url, check_encoding=True)

            # If the canonical URL is relative, make it absolute based on the source_url
            if not canonical_url.domain or not canonical_url.parsed.scheme:
                canonical_url = URL(urlparse.urljoin(self.source_url.url or "", canonical_url.url))

            # For now we force canonical URLs to be on the same domain. May relax carefully in the future
            elif self.source_url and canonical_url.domain != self.source_url.domain:
                return None

            return canonical_url
