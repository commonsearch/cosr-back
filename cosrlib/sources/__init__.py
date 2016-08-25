from cosrlib.document import load_document_type
from cosrlib.plugins import Plugin, load_plugin


def load_source(source_name, args, **kwargs):
    """ Loads and instanciates a Source class """

    if "." in source_name:
        cls_name = source_name
    else:
        cls_name = "cosrlib.sources.%s.%sSource" % (source_name, source_name.title())

    return load_plugin(cls_name, args, **kwargs)


class Source(Plugin):
    """ Base Source class, yields Documents """

    def __init__(self, args, plugins=None):

        Plugin.__init__(self, args)

        self.filter_url_hooks = []
        if plugins:
            self.filter_url_hooks = plugins.get("filter_url") or []

    def qualify_url(self, url):
        """ Return (True, index_level) if this URL is acceptable given the current filters """

        do_parse, do_index, do_index_body = True, False, False

        for _, hook in self.filter_url_hooks:
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

    def get_partitions(self):
        """ Lists all available partitions for this Source """
        return []

    def iter_items(self, partition):
        """ Yields objects in the source's native format """
        return []

    def iter_documents(self, partition):
        """ Yields *Document objects from one partition. This is the main public method for Sources """

        i = 0
        maxdocs = int(self.args.get("maxdocs") or 0)

        for url, headers, document_type, index_level, body in self.iter_items(partition):

            document = load_document_type(
                document_type,
                body,
                url=url,
                headers=headers,
                index_level=index_level
            )

            yield document

            i += 1
            if i > maxdocs > 0:
                return

    def iter_all_documents(self):
        """ Yields all Documents from all partitions """

        for partition in self.get_partitions():
            for document in self.iter_documents(partition):
                yield document
