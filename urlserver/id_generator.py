from __future__ import absolute_import, division, print_function, unicode_literals

import mmh3
from lru import LRU  # pylint: disable=no-name-in-module
from cosrlib.url import tld_extract

# TODO: run analytics on common crawl to get a larger list
COMMON_URLS = {
    "": 0,
    "/index.html": 1
}


URL_DOMAIN_IDS_CACHE = LRU(10000)


def make_url_id(url):
    """ Returns an int64 ID from a cosrlib.url.URL object """
    return (make_subdomain_path_query_id(url) << 32) + make_pld_id(url)


def make_domain_id(url):
    """ Returns an int64 ID from a cosrlib.url.URL object """

    if url.domain not in URL_DOMAIN_IDS_CACHE:
        URL_DOMAIN_IDS_CACHE[url.domain] = (make_subdomain_id(url) << 32) + make_pld_id(url)

    return URL_DOMAIN_IDS_CACHE[url.domain]


def make_pld_id(url):
    """ Returns an int32 ID from a cosrlib.url.URL object """

    # TODO: Move this to a globally incremented ID to avoid collisions
    return mmh3.hash(url.pld)


def make_subdomain_path_query_id(url):
    """ Returns an int32 ID from a cosrlib.url.URL object """
    path = url.normalized_subdomain + url.normalized_path + url.parsed.query
    lookup = COMMON_URLS.get(path)
    if lookup is not None:
        return lookup
    else:
        return mmh3.hash(path)


def make_subdomain_id(url):
    """ Returns an int32 ID from a cosrlib.url.URL object """
    if url.normalized_subdomain == "":
        return 0
    return mmh3.hash(url.normalized_subdomain)


def _fast_make_domain_id(host):
    """ Experimental fast version bypassing cosrlib.URL
        Note: not compatible with make_domain_id"""

    if host not in URL_DOMAIN_IDS_CACHE:

        subdomain, domain, suffix = tld_extract(host)

        if subdomain == "www" or not subdomain:
            URL_DOMAIN_IDS_CACHE[host] = \
                mmh3.hash64("%s.%s" % (domain, suffix))[0]
        else:
            while subdomain.startswith("www."):
                subdomain = subdomain[4:]

            URL_DOMAIN_IDS_CACHE[host] = \
                mmh3.hash64("%s.%s.%s" % (subdomain, domain, suffix))[0]

    return URL_DOMAIN_IDS_CACHE[host]
