from cosrlib.document import load_document_type
from cosrlib.plugins import Plugin, load_plugin


def load_source(source_name, args):
    """ Loads and instanciates a Source class """

    cls_name = "cosrlib.sources.%s.%sSource" % (source_name, source_name.title())
    return load_plugin(cls_name, args)


class Source(Plugin):
    """ Base Source class, yields Documents """

    def filter_url(self, url):
        """ Returns True if this URL is acceptable given the current filters """

        filters = self.args.get("filters")

        if filters:

            if filters.get("domain_whitelist"):
                if url.domain not in filters["domain_whitelist"]:
                    return False

            elif filters.get("domain_blacklist"):
                if url.domain in filters["domain_blacklist"]:
                    return False

            elif filters.get("only_homepages"):
                if url.parsed.path != "/" or url.parsed.query != "":
                    return False

        return True

    def iter_items(self):
        """ Yields objects in the source's native format """
        return []

    def iter_documents(self):
        """ Yields *Document objects. This is the main public method for Sources """

        for url, headers, document_type, body in self.iter_items():

            document = load_document_type(document_type, body, url=url, headers=headers)

            yield document
