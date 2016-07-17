import mmh3


# TODO: run analytics on common crawl to get a larger list
COMMON_URLS = {
    "": 0,
    "/index.html": 1
}


def make_url_id(url):
    """ Returns an int64 ID from a cosrlib.url.URL object """
    return (make_subdomain_path_query_id(url) << 32) + make_pld_id(url)


def make_domain_id(url):
    """ Returns an int64 ID from a cosrlib.url.URL object """
    return (make_subdomain_id(url) << 32) + make_pld_id(url)


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
    return mmh3.hash(url.normalized_subdomain)
