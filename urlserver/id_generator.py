import mmh3


def make_url_id(url):
    """ Returns an int64 ID from a cosrlib.url.URL object """
    return mmh3.hash64(url.normalized)[0]


def make_domain_id(url):
    """ Returns an int64 ID from a cosrlib.url.URL object """
    return mmh3.hash64(url.normalized_domain)[0]
