from cosrlib.plugins import Plugin
from cosrlib import re


class FilterPlugin(Plugin):
    """ Abstract class for plugins that filter URLs """

    hooks = frozenset(["filter_url"])

    def __init__(self, args):
        Plugin.__init__(self, args)
        self.do_parse = bool(int(self.args.get("parse", "1")))
        self.do_index = bool(int(self.args.get("index", "1"))) and self.do_parse
        self.do_index_body = bool(int(self.args.get("index_body", "1"))) and self.do_index

    def filter_url(self, url):
        """ Returns what to do with this URL: (do_parse, do_index, do_index_body) """

        if self.match_url(url):
            return (self.do_parse, self.do_index, self.do_index_body)
        return (None, None, None)

    def match_url(self, url):
        return True


class All(FilterPlugin):
    """ Filters all documents """

    def match_url(self, url):
        return True


class Homepages(FilterPlugin):
    """ Filters homepages """

    def match_url(self, url):
        return (url.parsed.path == "/" and url.parsed.query == "")


class Domains(FilterPlugin):
    """ Domain filter """

    def init(self):
        # Match based on domain suffixes
        self.regex_source = "|".join([re.escape(d) + "$" for d in self.args["domains"].split(",")])
        self.regex = re.compile(self.regex_source)

    def match_url(self, url):
        return self.regex.search(url.domain)
