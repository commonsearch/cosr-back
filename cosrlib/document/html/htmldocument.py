import urlparse

import gumbocy

from cosrlib import re
from cosrlib.url import URL
from cosrlib.document import Document

from . import defs
from .htmlencoding import HTMLEncoding


_RE_SEARCH_STYLE_HIDDEN = re.compile(r"(display\s*\:\s*none)|(visibility\s*\:\s*hidden)")
_RE_STRIP_TAGS = re.compile(r"<.*?>")


class HTMLDocument(Document):
    """ Class representing an HTML document, with methods for parsing and extracting content """

    # pylint: disable=attribute-defined-outside-init

    def __init__(self, *args, **kwargs):
        Document.__init__(self, *args, **kwargs)

        self.encoding = HTMLEncoding(self)
        self.parser = None

    def discard_source_data(self):
        """ Remove source_data from memory """
        del self.source_data
        del self.parser

    def reset(self):
        """ Reset the tree traversal properties """

        # Store infos found in the <head> for further reuse
        self.head_links = []
        self.head_metas = {}

        # Base URL for the document if a <base> tag is present
        # https://developer.mozilla.org/en-US/docs/Web/HTML/Element/base
        self.base_url = None

        # Current level in the tree
        self.current_level = -1

        # Current stack of parent tags, a list of (tag_name, weight) tuples
        self.current_stack = []

        # Words in the currently open word group. None if no group open
        self.current_words = None

        # Integer levels where some attributes for the whole subtree start
        self.level_boilerplate = None
        self.level_boilerplate_bypass = None
        self.level_hyperlink = None
        self.level_hidden = None
        self.level_head = None
        self.level_article = None

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

        self.create_parser()

        self._start_word_group({"key": None, "weight": 1, "tag": "html"})

        nodes = self.parser.listnodes(defs.GUMBOCY_OPTIONS)

        for node in nodes:

            # node is a tuple: (level, tag_name, attributes) or (level, None, text_content)

            # print " " * node[0], node

            # This new node is higher in the tree than the previous one: close some tags!
            if node[0] <= self.current_level:
                for _ in range(self.current_level + 1 - node[0]):
                    self._close_tag()

            # TODO: Is this a valid optimization? While we're under hidden elements, ignore nodes
            if self.level_hidden is not None and node[0] >= self.level_hidden:
                continue

            # Text node
            if node[1] is None:
                self._add_text(node[2])

            # Element node
            else:
                self._start_tag(*node)
                self.current_level += 1

        # Close remaining tags
        for _ in range(self.current_level + 1):
            self._close_tag()

        self._close_word_group()

        return self

    def create_parser(self):
        """ Creates a gumbocy parser on self.source_data """

        if self.parser is None:
            self.parser = gumbocy.HTMLParser(self.source_data)
            self.parser.parse()
            self.reset()

    def _start_word_group(self, word_group_options):
        """ Starts collecting words under the current element """

        self._close_word_group()
        self.current_word_group = word_group_options
        self.current_words = []

    def _close_word_group(self):
        """ Stops collecting words under the current element """

        if self.current_words:
            self.add_word_group(" ".join(self.current_words), **self.current_word_group)
        self.current_words = None

    def _start_tag(self, level, tag_name, attrs=None):
        """ We parsed a new element node in the document tree. """

        attrs = attrs or {}

        # TODO: different weights (h1-h6 tags for instance)
        weight = 1

        # Add this new element to the stack
        self.current_stack.append((tag_name, weight))

        # TODO: If se wee role=presentation, should we transform the tag_name into a simple span?
        # https://www.w3.org/TR/wai-aria/roles#presentation

        if tag_name == "head":
            self.level_head = level
            return

        if tag_name == "article":
            self.level_article = level

        # If we are in the HEAD, only metadata should be interesting
        if self.level_head is not None:

            if tag_name == "link":
                self.head_links.append(attrs)

            elif tag_name == "meta":
                meta_name = (attrs.get("name") or attrs.get("property") or "").lower()
                if meta_name in defs.META_WHITELIST:
                    self.head_metas[meta_name] = (attrs.get("content") or "").strip()

            elif tag_name == "base" and attrs.get("href") and self.base_url is None:
                self.base_url = URL(attrs["href"])

        # If we are in the BODY, inspect things a bit more
        else:

            if tag_name == "img":
                self._close_word_group()
                if attrs.get("alt"):
                    self.add_word_group(attrs["alt"], tag="img")
                if attrs.get("src"):
                    if not attrs["src"].startswith("data:"):
                        self.add_word_group(" ".join(self._split_filename_words(attrs["src"])), tag="img")

            # Does this element start a hidden subtree?
            if self.level_hidden is None and attrs and self._guess_element_hidden(attrs):
                self.level_hidden = level

            # Are we in a boilerplate subtree with a bypass tag? (like a title in a header)
            if self.level_boilerplate is not None and self.level_boilerplate_bypass is None:
                if tag_name in defs.TAGS_BOILERPLATE_BYPASS:
                    self.level_boilerplate_bypass = level

            # Does this element start a boilerplate subtree?
            if self.level_boilerplate is None and self._guess_element_boilerplate(attrs):
                self.level_boilerplate = level

            # Does this element start a hyperlink subtree?
            # TODO how to deal with nested a?
            if (
                    tag_name == "a" and
                    attrs.get("rel") != "nofollow" and
                    attrs.get("href") and
                    not attrs["href"].startswith("javascript:") and
                    not attrs["href"].startswith("mailto:")
            ):
                self.level_hyperlink = level
                self.link_words = []
                self.link_href = attrs.get("href")

            # does this element break word groups?
            is_separator = tag_name in defs.TAGS_SEPARATORS

            # Do we want to start collecting words?
            # TODO: would we avoid this for elements with no children?
            if self.level_hidden is None and self.level_boilerplate is None and is_separator:
                toplevel_word_group = {"tag": tag_name, "weight": weight}
                self._start_word_group(toplevel_word_group)

    def _add_text(self, text):
        """ We parsed a new text node in the document tree. """

        tag_name, weight = self.current_stack[-1]

        if tag_name == "title" and not self._title:
            self._title = text
            return

        if self.level_hidden is None:
            words = self._split_words(text)

            # Collect hyperlink text
            if self.level_hyperlink is not None:
                self.link_words += words

            if self.level_boilerplate is None or self.level_boilerplate_bypass is not None:

                # If no word group is currently open, we have to re-open one for the current tag.
                if self.current_words is None:
                    self._start_word_group({"tag": tag_name, "weight": weight})

                # Collect current words!
                self.current_words += words

    def _close_tag(self):
        """ Close the current tag """

        closed_tag = self.current_stack.pop(-1)

        is_separator = closed_tag[0] in defs.TAGS_SEPARATORS

        if self.level_boilerplate == self.current_level:
            self.level_boilerplate = None

        if self.level_boilerplate_bypass == self.current_level:
            self.level_boilerplate_bypass = None

        if self.level_hidden == self.current_level:
            self.level_hidden = None

        if self.level_head == self.current_level:
            self.level_head = None

        if self.level_article == self.current_level:
            self.level_article = None

        if self.level_hyperlink == self.current_level:
            self.add_hyperlink(self.link_href, self.link_words)
            self.link_words = []
            self.link_href = None
            self.level_hyperlink = None

        if self.level_hidden is None and self.level_boilerplate is None and is_separator:
            self._close_word_group()

        self.current_level -= 1

    def _guess_element_boilerplate(self, attrs):
        """ Rough guess to check if the element is boilerplate """

        if self.current_stack[-1][0] in defs.TAGS_BOILERPLATE:
            return True

        # http://html5doctor.com/understanding-aside/
        if self.current_stack[-1][0] == "aside" and self.level_article is None:
            return True

        if not attrs:
            return False

        if attrs.get("class"):
            for k in attrs.get("class"):
                if k in defs.CLASSES_BOILERPLATE:
                    return True

        if attrs.get("id") and attrs["id"].lower() in defs.IDS_BOILERPLATE:
            return True

        if attrs.get("role") and attrs["role"].lower() in defs.ROLES_BOILERPLATE:
            return True

        return False

    def _guess_element_hidden(self, attrs):
        """ Rough guess to check if the element is explicitly hidden.

            Not intended to combat spam!
        """

        # From the HTML5 spec
        if "hidden" in attrs:
            return True

        if attrs.get("aria-hidden") == "true":
            return True

        if attrs.get("id") and attrs["id"].lower() in defs.IDS_HIDDEN:
            return True

        if attrs.get("class"):
            for k in attrs.get("class"):
                if k in defs.CLASSES_HIDDEN:
                    return True

        if attrs.get("style") and _RE_SEARCH_STYLE_HIDDEN.search(attrs["style"]):
            return True

        return False

    # TODO: add metadata like nofollow & other types of links
    def add_hyperlink(self, href, words):
        """ Validate then add an hyperlink parsed from the document """

        # Resolve relative links. Some might have a weird syntax so we need to
        # catch exceptions.
        try:
            url = URL((self.base_url or self.source_url).urljoin(href), check_encoding=True)
        except ValueError:
            return

        if url.parsed.scheme in ("http", "https"):
            self._hyperlinks.append({"href": url, "words": words})

    def parse_canonical_url(self):
        """ Look for a valid meta tag rel="canonical" in the HEAD of the page.
            Returns an absolute URL in all cases """

        # TODO: should we use og:url as canonical url? http://ogp.me/

        meta_rel_canonical = [x for x in self.head_links if (x.get("rel") or "").lower() == "canonical"]
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
