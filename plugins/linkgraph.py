from cosrlib.plugins import Plugin


class DomainToDomain(Plugin):
    """ Saves a graph of domain=>domain links """

    hooks = frozenset(["document_post_index", "spark_pipeline_collect"])

    def document_post_index(self, document, spark_response):
        """ Filters a document post-indexing """

        spark_response["links"] = document.get_hyperlinks()

    def spark_pipeline_collect(self, sc, rdd, indexer):

        # @ignore_exceptions([])
        def iter_links_domain(record):
            """ Returns all the parsed links in this record as (from_domain, to_domain) tuples  """

            record_domain = indexer.urlclient.get_domain_ids([record["url"]])[0]
            domains = list(set([link["href"].homepage for link in record["links"]]))
            link_ids = set(indexer.urlclient.get_domain_ids(domains))
            link_ids.discard(record_domain)

            return [(record_domain, d) for d in link_ids]

        rdd = rdd.flatMap(iter_links_domain).distinct().map(lambda row: "%s %s" % row)

        if self.args.get("coalesce"):
            rdd = rdd.coalesce(int(self.args["coalesce"]), shuffle=bool(self.args.get("shuffle")))

        rdd.saveAsTextFile(self.args["dir"])
