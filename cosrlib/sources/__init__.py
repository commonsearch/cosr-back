from cosrlib.document import load_document_type
from cosrlib.plugins import Plugin, load_plugin


def load_source(source_name, args):
    """ Loads and instanciates a Source class """

    cls_name = "cosrlib.sources.%s.%sSource" % (source_name, source_name.title())
    return load_plugin(cls_name, args)


class Source(Plugin):
    """ Base Source class, yields Documents """

    def __init__(self, args):

        Plugin.__init__(self, args)

        self.filter_url_hooks = []
        if self.args.get("plugins"):
            self.filter_url_hooks = self.args["plugins"].get("filter_url") or []

    def filter_url(self, url):
        """ Returns True if this URL is acceptable given the current filters """

        do_parse, do_index, do_index_body = True, True, True

        for hook in self.filter_url_hooks:
            res = hook(url)

            if res[0] is not None:
                do_parse = res[0]
            if res[1] is not None:
                do_index = res[1]
            if res[2] is not None:
                do_index_body = res[2]

        if do_index_body:
            index_level = 2
        elif do_index:
            index_level = 1
        else:
            index_level = 0

        return do_parse, index_level

    def iter_items(self):
        """ Yields objects in the source's native format """
        return []

    def iter_documents(self):
        """ Yields *Document objects. This is the main public method for Sources """

        i = 0
        maxdocs = int(self.args.get("maxdocs") or 9999999999)

        for url, headers, document_type, index_level, body in self.iter_items():

            document = load_document_type(
                document_type,
                body,
                url=url,
                headers=headers,
                index_level=index_level
            )

            yield document

            i += 1
            if i > maxdocs:
                return
