# pylint: disable=too-many-branches
import urllib
# import tldextract as _tldextract
import urlparse4 as urlparse

from pyfaup.faup import Faup


def tld_extract(domain):

    if "_faup" not in __builtins__:
        __builtins__["_faup"] = Faup()
    _faup = __builtins__["_faup"]
    _faup.decode(domain.decode("utf-8").strip("."))
    return (_faup.get_subdomain() or "", _faup.get_domain_without_tld() or "", _faup.get_tld() or "")

# TODO init lazily
# _tldextractor = _tldextract.TLDExtract(suffix_list_urls=None)


class URL(object):
    """ Base class for manipulating an URL without context """

    def __init__(self, url, check_encoding=False):

        if isinstance(url, unicode):
            self.url = url.encode("utf-8")
        else:
            self.url = url

        if check_encoding:
            try:
                self.url.decode('ascii')
            except UnicodeDecodeError:
                p = urlparse.urlsplit(self.url)

                # TODO: check the rightfulness of this!
                self.url = urlparse.urlunsplit((
                    p[0],
                    p[1],
                    urllib.quote(p[2], safe="/"),
                    urllib.quote(p[3], safe="&?="),
                    urllib.quote(p[4])
                ))

    def urljoin(self, href):
        """ Optimized version of urlparse.urljoin() """
        return urlparse.urljoin(self.url, href)

    # Allow picking/unpickling
    def __getstate__(self):
        return self.url

    def __setstate__(self, state):
        self.url = state

    # This is only called when the attribute is still missing
    def __getattr__(self, attr):
        # pylint: disable=redefined-variable-type

        if attr == "parsed":
            # try:
            value = urlparse.urlsplit(self.url)
            # except ValueError:
            #     value = urlparse.urlsplit("about:blank")

        elif attr == "tldextracted":

            value = tld_extract(self.parsed.netloc)
            # value = _tldextractor(self.url)

        elif attr == "normalized":
            value = urlparse.urlunsplit((
                None,
                self.normalized_domain,
                self.parsed.path if self.parsed.path else "/",
                self.parsed.query,
                ""
            )).lstrip("/")

            if value.count("/") == 1:
                value = value.strip("/")

        elif attr == "normalized_without_query":
            value = urlparse.urlunsplit((
                None,
                self.normalized_domain,
                self.parsed.path if self.parsed.path else "/",
                "",
                ""
            )).lstrip("/")

            if value.count("/") == 1:
                value = value.strip("/")

        elif attr == "homepage":
            value = urlparse.urlunsplit((
                self.parsed.scheme,
                self.domain,
                "/",
                "",
                ""
            )).strip("/")

        # Pay-level domain
        elif attr == "pld":
            value = "%s.%s" % (self.tldextracted[1], self.tldextracted[2])

        elif attr == "domain":
            value = self.parsed.netloc

        elif attr == "subdomain":
            value = self.tldextracted[0]

        elif attr == "normalized_domain":

            value = self.domain.strip(".")

            while value.startswith("www."):
                value = value[4:]

            if value.endswith(':80'):
                value = value[:-3]
            elif value.endswith(':443'):
                value = value[:-4]

            value = value.strip(".")

        elif attr == "normalized_subdomain":

            value = self.subdomain.strip(".")

            if value == "www":
                value = ""
            else:
                while value.startswith("www."):
                    value = value[4:]

        elif attr == "normalized_path":
            if self.parsed.path == "/":
                return ""
            return self.parsed.path

        # https://en.wikipedia.org/wiki/Public_Suffix_List
        # Returns the domain name suffix ("co.uk" for "bbc.co.uk")
        elif attr == "suffix":
            value = self.tldextracted[2]

        else:
            raise Exception("Unknown attribute %s !" % attr)

        self.__dict__[attr] = value
        return value
