from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from pyspark.sql import types as SparkTypes

from cosrlib.url import URL
from cosrlib.spark import createDataFrame, sql, SparkPlugin
from cosrlib import re, py2_long
from urlserver.id_generator import _fast_make_domain_id


_RE_STRIP_FRAGMENT = re.compile(r"#.*")


class WebGraphPlugin(SparkPlugin):
    """ Base class for WebGraph plugins """

    include_external = True
    include_internal = True

    def hook_spark_pipeline_init(self, sc, sqlc, schema, indexer):

        if self.include_external:
            schema.append(
                SparkTypes.StructField("external_links", SparkTypes.ArrayType(SparkTypes.StructType([
                    SparkTypes.StructField("href", SparkTypes.StringType(), nullable=False),
                    SparkTypes.StructField("text", SparkTypes.StringType(), nullable=True)
                ])), nullable=True)
            )

        if self.include_internal:
            schema.append(
                SparkTypes.StructField("internal_links", SparkTypes.ArrayType(SparkTypes.StructType([
                    SparkTypes.StructField("path", SparkTypes.StringType(), nullable=False),
                    SparkTypes.StructField("text", SparkTypes.StringType(), nullable=True)
                ])), nullable=True)
            )

    def hook_document_post_index(self, document, metadata):
        """ Collect all unique normalized external URLs """

        if self.include_external:
            seen = set()
            for link in document.get_external_hyperlinks(exclude_nofollow=self.exclude_nofollow):
                key = (link["href"].normalized, link["text"])
                if key not in seen:
                    seen.add(key)
                    metadata.setdefault("external_links", [])
                    metadata["external_links"].append(key)

        if self.include_internal:
            seen = set()
            metadata["internal_links"] = []
            for link in document.get_internal_hyperlinks():  # exclude_nofollow=self.exclude_nofollow):
                key = (_RE_STRIP_FRAGMENT.sub("", link["path"]), link["text"])
                if key not in seen:
                    seen.add(key)
                    metadata.setdefault("internal_links", [])
                    metadata["internal_links"].append(key)

    def init(self):

        self.exclude_nofollow = (self.args.get("include_nofollow") != "1")

        if self.args.get("output"):
            if os.path.isdir(os.path.join(self.args["output"], "edges")):
                shutil.rmtree(os.path.join(self.args["output"], "edges"))
            if os.path.isdir(os.path.join(self.args["output"], "vertices")):
                shutil.rmtree(os.path.join(self.args["output"], "vertices"))


class DomainToDomain(WebGraphPlugin):
    """ Saves a graph of domain=>domain links in text format """

    include_internal = False

    def hook_spark_pipeline_action(self, sc, sqlc, df, indexer):

        # Get all unique (host1 => host2) pairs
        domain_pairs = sql(sqlc, """
            SELECT parse_url(url, "HOST") as d1, parse_url(CONCAT("http://", link), "HOST") as d2
            FROM (
                SELECT url, EXPLODE(external_links.href) as link FROM df
            ) as pairs
        """, {"df": df}).distinct()

        # Format as csv
        lines = sql(sqlc, """
            SELECT CONCAT(d1, " ", d2) as r
            FROM pairs
        """, {"pairs": domain_pairs})

        self.save_dataframe(lines, "text")

        return True


class DomainToDomainParquet(WebGraphPlugin):
    """ Saves a graph of domain=>domain links in Apache Parquet format """

    include_internal = False

    def hook_spark_pipeline_action(self, sc, sqlc, df, indexer):

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
            SELECT parse_url(CONCAT("http://", link), "HOST") as domain
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

            return [(py2_long(_id), str(name))]

        rdd_domains = all_domains_df.rdd.flatMap(iter_domain)

        vertex_df = createDataFrame(sqlc, rdd_domains, vertex_graph_schema).distinct()

        coalesce = int(self.args.get("coalesce_vertices") or self.args.get("coalesce", 1) or 0)
        if coalesce > 0:
            vertex_df = vertex_df.coalesce(coalesce)

        vertex_df.write.parquet(os.path.join(self.args["output"], "vertices"))

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
            SELECT parse_url(url, "HOST") as d1, parse_url(CONCAT("http://", link), "HOST") as d2
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
                return [(py2_long(from_domain), py2_long(to_domain))]

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

        coalesce = int(self.args.get("coalesce_edges") or self.args.get("coalesce", 1) or 0)
        if coalesce > 0:
            weighted_edge_df = weighted_edge_df.coalesce(coalesce)

        weighted_edge_df.write.parquet(os.path.join(self.args["output"], "edges"))
