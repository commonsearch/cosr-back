from cosrlib import re


_RE_WHITESPLACE = re.compile(r"\s+")
_RE_REMOVE_LAST_WORD = re.compile(r"\s([^\s]*)$")

# Some titles are useless and should be replaced by something more relevant
BLACKLISTED_TITLES = frozenset([
    "home",
    "default"
])

# Some summaries are useless and should be replaced by something more relevant
BLACKLISTED_SUMMARIES = frozenset([
    "default"
])

# Maximum length for titles
TITLE_MAX_LENGTH = 70

# Maximum length for summaries
SUMMARY_MAX_LENGTH = 160


def unicode_truncate(s, length, keep_words=False, ellipsis=u"..."):
    """ Truncates an UTF-8 string and return it as unicode """

    encoded = s.decode("utf-8", "ignore")

    # If the unicode form is already under the length, return directly
    if len(encoded) <= length:
        return encoded

    # Now we really need to truncate
    if keep_words:
        encoded = _RE_REMOVE_LAST_WORD.sub("", encoded[:length + 1])

    encoded = encoded[:length] + ellipsis

    return encoded


def _is_invalid_title(title):
    """ Is this title good enough for display in search results? """
    if not title or not title.strip():
        return True

    if title.strip().lower() in BLACKLISTED_TITLES:
        return True


def format_title(document, url_metadata):  # pylint: disable=unused-argument
    """ Returns a document title properly formatted for SERP display """

    title = document.get_title()

    # Try the Open Graph Protocol title http://ogp.me/
    if _is_invalid_title(title):
        title = document.head_metas.get("og:title")

    # TODO: Look in DMOZ with url_metadata

    # Last fallback: use the domain name!
    if _is_invalid_title(title):

        domain_paid_words = document.get_domain_paid_words()
        if domain_paid_words:
            title = (" ".join(domain_paid_words)).title()
        else:
            return ""

    tokens = _RE_WHITESPLACE.split(title.strip())
    cleaned = " ".join(tokens)

    return unicode_truncate(cleaned, TITLE_MAX_LENGTH, keep_words=True)


def _is_invalid_summary(summary):
    """ Is this summary good enough for display in search results? """
    if not summary or not summary.strip():
        return True

    if summary.strip().lower() in BLACKLISTED_SUMMARIES:
        return True


def format_summary(document, url_metadata):  # pylint: disable=unused-argument
    """ Returns a document summary properly formatted for SERP display """

    summary = document.head_metas.get("description")

    # Try the Open Graph Protocol description http://ogp.me/
    if _is_invalid_summary(summary):
        summary = document.head_metas.get("og:description")

    # TODO: Look in DMOZ with url_metadata

    # Fallback #1: try the first suitable group of words in relevant elements
    if _is_invalid_summary(summary):
        acceptable_tags = frozenset(["h1", "h2", "h3", "h4", "h5", "h6", "p"])
        for wg in document.get_word_groups():

            if wg.get("tag") in acceptable_tags:
                w = wg["words"]
                if len(w) > 40:
                    summary = w
                    break

    # Fallback #2: pick any word group longer than N characters!
    if _is_invalid_summary(summary):
        for wg in document.get_word_groups():
            w = wg["words"]
            if len(w) > 40:
                summary = w
                break

    if _is_invalid_summary(summary):
        return ""

    tokens = _RE_WHITESPLACE.split(summary.strip())
    cleaned = " ".join(tokens)

    return unicode_truncate(cleaned, SUMMARY_MAX_LENGTH, keep_words=True)
