import os
import shutil

from pyspark.sql import types as SparkTypes

from cosrlib.plugins import Plugin
from cosrlib.url import URL
from cosrlib.spark import createDataFrame, sql
from urlserver.id_generator import _fast_make_domain_id


class DomainToDomain(Plugin):
    """ Saves a graph of domain=>domain links in text format """

    hooks = frozenset(["document_post_index", "spark_pipeline_action", "spark_pipeline_init"])

    def spark_pipeline_init(self, sc, sqlc, schema, indexer):
        schema.append(SparkTypes.StructField("external_links", SparkTypes.ArrayType(SparkTypes.StructType([
            SparkTypes.StructField("href", SparkTypes.StringType(), nullable=False)
            # TODO: link text
        ])), nullable=True))

    def document_post_index(self, document, metadata):
        """ Filters a document post-indexing """

        metadata["external_links"] = [
            {"href": row["href"].url} for row in document.get_external_hyperlinks()
        ]

    def init(self):
        if self.args.get("path"):
            if os.path.isdir(os.path.join(self.args["path"], "edges")):
                shutil.rmtree(os.path.join(self.args["path"], "edges"))
            if os.path.isdir(os.path.join(self.args["path"], "vertices")):
                shutil.rmtree(os.path.join(self.args["path"], "vertices"))

    def spark_pipeline_action(self, sc, sqlc, df, indexer):

        def iter_links_domain(record):
            """ Returns all the parsed links in this record as (from_domain, to_domain) tuples  """

            record_domain = URL(record["url"]).normalized_domain
            domains = list(set([
                URL(link["href"]).normalized_domain
                for link in record["external_links"]
            ]))

            return [(record_domain, d) for d in domains]

        rdd = df.rdd.flatMap(iter_links_domain).distinct().map(lambda row: "%s %s" % row)

        if self.args.get("coalesce"):
            rdd = rdd.coalesce(int(self.args["coalesce"]), shuffle=bool(self.args.get("shuffle")))

        codec = None
        if self.args.get("gzip"):
            codec = "org.apache.hadoop.io.compress.GzipCodec"

        rdd.saveAsTextFile(self.args["path"], codec)


class DomainToDomainParquet(DomainToDomain):
    """ Saves a graph of domain=>domain links in Apache Parquet format """

    def spark_pipeline_action(self, sc, sqlc, df, indexer):

        self.save_vertex_graph(sqlc, df)
        self.save_edge_graph(sqlc, df)

        return True

    def save_vertex_graph(self, sqlc, df):
        """ Transforms a document metadata DataFrame into a Parquet dump of the vertices of the webgraph """

        vertex_graph_schema = SparkTypes.StructType([
            SparkTypes.StructField("id", SparkTypes.LongType(), nullable=False),
            SparkTypes.StructField("domain", SparkTypes.StringType(), nullable=False)
        ])

        # TODO ?!
        if self.args.get("shuffle_partitions"):
            sqlc.setConf("spark.sql.shuffle.partitions", self.args["shuffle_partitions"])

        # We collect all unique domains from the page URLs & destination of all external links
        d1_df = sql(sqlc, """
            SELECT parse_url(url, "HOST") as domain from df
        """, {"df": df}).distinct()

        d2_df = sql(sqlc, """
            SELECT parse_url(link, "HOST") as domain
            FROM (
                SELECT EXPLODE(external_links.href) as link FROM df
            ) as pairs
        """, {"df": df})

        all_domains_df = d1_df.unionAll(d2_df).distinct()

        def iter_domain(record):
            """ Transforms Row(domain=www.example.com) into tuple([int64 ID], "example.com") """

            domain = record["domain"]
            if not domain or not domain.strip():
                return []

            name = URL("http://" + domain).normalized_domain

            try:
                _id = _fast_make_domain_id(name)
            except Exception:  # pylint: disable=broad-except
                return []

            if name.startswith(".") or name.startswith("www."):
                print "HEYO %s %s %s" % (repr(name), repr(domain), repr(record))
                raise Exception("HEYO %s %s %s" % (repr(name), repr(domain), repr(record)))

            return [(long(_id), str(name))]

        rdd_domains = all_domains_df.rdd.flatMap(iter_domain)

        vertex_df = createDataFrame(sqlc, rdd_domains, vertex_graph_schema).distinct()

        if self.args.get("coalesce_vertices") or self.args.get("coalesce"):
            vertex_df = vertex_df.coalesce(
                int(self.args.get("coalesce_vertices") or self.args.get("coalesce"))
            )

        vertex_df.write.parquet(os.path.join(self.args["path"], "vertices"))

    def save_edge_graph(self, sqlc, df):
        """ Transforms a document metadata DataFrame into a Parquet dump of the edges of the webgraph """

        edge_graph_schema = SparkTypes.StructType([
            SparkTypes.StructField("src", SparkTypes.LongType(), nullable=False),
            SparkTypes.StructField("dst", SparkTypes.LongType(), nullable=False),

            # Sum of weights must be 1
            # This field will automatically be added by the SQL query
            # SparkTypes.StructField("weight", SparkTypes.FloatType(), nullable=True)
        ])

        # TODO?!
        if self.args.get("shuffle_partitions"):
            sqlc.setConf("spark.sql.shuffle.partitions", self.args["shuffle_partitions"])

        # Get all unique (host1 => host2) pairs
        new_df = sql(sqlc, """
            SELECT parse_url(url, "HOST") as d1, parse_url(link, "HOST") as d2
            FROM (
                SELECT url, EXPLODE(external_links.href) as link FROM df
            ) as pairs
        """, {"df": df}).distinct()

        def iter_links_domain(record):
            """ Transforms Row(d1="x.com", d2="y.com") into tuple([int64 ID], [int64 ID]) """

            d1 = record["d1"]
            d2 = record["d2"]
            if not d1 or not d2:
                return []

            try:
                from_domain = _fast_make_domain_id(d1)
                to_domain = _fast_make_domain_id(d2)
            except Exception:  # pylint: disable=broad-except
                return []

            if from_domain == to_domain:
                return []
            else:
                return [(long(from_domain), long(to_domain))]

        rdd_couples = new_df.rdd.flatMap(iter_links_domain)

        edge_df = createDataFrame(sqlc, rdd_couples, edge_graph_schema).distinct()

        # After collecting all the unique (from_id, to_id) pairs, we add the weight of every edge
        # The current algorithm is naive: edge weight is equally split between all the links, with
        # the sum of all weights for a source domain always = 1.
        weights_df = sql(sqlc, """
            SELECT src id, cast(1 / count(*) as float) weight
            FROM edges
            GROUP BY src
        """, {"edges": edge_df})

        weighted_edge_df = sql(sqlc, """
            SELECT cast(src as long) src, cast(dst as long) dst, cast(weights.weight as float) weight
            FROM edges
            JOIN weights on edges.src = weights.id
        """, {"edges": edge_df, "weights": weights_df})

        if self.args.get("coalesce_edges") or self.args.get("coalesce"):
            weighted_edge_df = weighted_edge_df.coalesce(
                int(self.args.get("coalesce_edges") or self.args.get("coalesce"))
            )

        weighted_edge_df.write.parquet(os.path.join(self.args["path"], "edges"))
