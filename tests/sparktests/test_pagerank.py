import pytest
import os
import tempfile
import shutil
import json


# Examples from Ian Rogers
# http://www.sirgroane.net/google-page-rank/
GRAPHS = {

    # Format of the graph is:
    # "vertex_name": (expected_pagerank, [list of vertices linked to])

    "ian-rogers-00": {
        "graph": {
            "a": (1.0, ["b"]),
            "b": (1.0, ["a"])
        },
        "approx": 0.01
    },

    "ian-rogers-01": {
        "graph": {
            "a": (1.49, ["b", "c"]),
            "b": (0.78, ["c"]),
            "c": (1.58, ["a"]),
            "d": (0.15, ["c"])
        },
        "approx": 0.01
    },

    "ian-rogers-02": {
        "graph": {
            "home": (0.92, ["about", "product", "links"]),
            "about": (0.41, ["home"]),
            "product": (0.41, ["home"]),
            "links": (0.41, ["home", "a", "b", "c", "d"]),
            "a": (0.22, []),
            "b": (0.22, []),
            "c": (0.22, []),
            "d": (0.22, [])
        },
        "approx": 0.01
    },

    "ian-rogers-03": {
        "graph": {
            "home": (3.35, ["about", "product", "links"]),
            "about": (1.1, ["home"]),
            "product": (1.1, ["home"]),
            "links": (1.1, ["home", "a", "b", "c", "d"]),
            "a": (0.34, ["home"]),
            "b": (0.34, ["home"]),
            "c": (0.34, ["home"]),
            "d": (0.34, ["home"])
        },
        "approx": 0.01
    },

    "ian-rogers-04": {
        "graph": {
            "home": (2.44, ["about", "product", "links"]),
            "about": (0.84, ["home"]),
            "product": (0.84, ["home"]),
            "links": (0.84, ["home", "a", "b", "c", "d", "ra", "rb", "rc", "rd"]),
            "a": (0.23, []),
            "b": (0.23, []),
            "c": (0.23, []),
            "d": (0.23, []),
            "ra": (0.23, ["home"]),
            "rb": (0.23, ["home"]),
            "rc": (0.23, ["home"]),
            "rd": (0.23, ["home"]),

        },
        "approx": 0.01
    },

    "ian-rogers-05": {
        "graph": {
            "home": (1.92, ["about", "product", "more"]),
            "about": (0.69, ["home"]),
            "product": (0.69, ["home"]),
            "more": (0.69, ["home"])
        },
        "approx": 0.01
    },

    "ian-rogers-06": {
        "graph": {
            "home": (1.0, ["about"]),
            "about": (1.0, ["product"]),
            "product": (1.0, ["more"]),
            "more": (1.0, ["home"])
        },
        "approx": 0.01
    },

    "ian-rogers-07": {
        "graph": {
            "home": (1.0, ["about", "product", "more"]),
            "about": (1.0, ["product", "home", "more"]),
            "product": (1.0, ["more", "about", "home"]),
            "more": (1.0, ["home", "about", "product"])
        },
        "approx": 0.01
    },

    # 8 to 10 rely on a "Site A" with an artificially fixed PageRank, so we can't include them here.

    "ian-rogers-11": {
        "graph": {
            "a": (1.44, ["b"]),
            "b": (1.37, ["a", "c"]),
            "c": (0.73, ["a", "d"]),
            "d": (0.46, ["a"])
        },
        "approx": 0.01
    },

    "ian-rogers-12": {
        "graph": {
            "a": (1.20, ["b"]),
            "b": (1.44, ["a", "c"]),
            "c": (0.94, ["a", "d", "b"]),
            "d": (0.41, ["a", "c"])
        },
        "approx": 0.01
    },

    "ian-rogers-13": {
        "graph": dict({
            "a": (331.1, ["b"]),
            "b": (281.6, ["spam%d" % i for i in range(1000)])
        }, **{
            ("spam%d" % i): (0.39, ["a"])
            for i in range(1000)
        }),
        "maxiter": 200,
        "approx": 0.1
    }

}

IMPLEMENTATIONS = {
    "sparksql": {
        "default_maxiter": 100,
        "default_tol": 0
    },
    "graphframes": {
        "default_maxiter": 100,
        "default_tol": 0
    },
    "rdd": {
        "default_maxiter": 100,
        "default_tol": 0
    }
}


# Skipped until https://issues.apache.org/jira/browse/SPARK-16802 is fixed
@pytest.mark.skip
@pytest.mark.parametrize(("p_graph_name", "p_implementation"), [
    (graph_name, implementation)
    for graph_name in sorted(GRAPHS.keys())
    for implementation in sorted(IMPLEMENTATIONS.keys())
])
def test_pagerank_computation(p_graph_name, p_implementation, sparksubmit):

    p_graph = GRAPHS[p_graph_name]

    corpus = {"docs": [
        {
            "url": "http://%s.com" % name,
            "content": " ".join(["<a href='http://%s.com'>%s</a>" % (link, link) for link in data[1]])
        }
        for name, data in p_graph["graph"].iteritems()
    ]}

    maxiter = p_graph.get("maxiter", IMPLEMENTATIONS[p_implementation]["default_maxiter"])
    tol = p_graph.get("tol", IMPLEMENTATIONS[p_implementation]["default_tol"])

    webgraph_dir = tempfile.mkdtemp()

    try:

        corpus_file = os.path.join(webgraph_dir, "_corpus.json")
        with open(corpus_file, "w") as f:
            f.write(json.dumps(corpus["docs"]))

        sparksubmit("spark/jobs/pipeline.py --source corpus:path=%s,persist=1 --plugin plugins.webgraph.DomainToDomainParquet:coalesce=4,shuffle_partitions=1,output=%s/out/" % (
            corpus_file,
            webgraph_dir
        ))

        # Now compute page rank on the domain graph!
        sparksubmit("spark/jobs/pagerank.py --implementation %s --webgraph %s/out/ --output %s/out/pagerank/  --maxiter %s --shuffle_partitions 4 --tol %s --stats 1 --include_orphans" % (
            p_implementation,
            webgraph_dir,
            webgraph_dir,
            maxiter,
            tol
        ))

        # We collect(1) so there should be only one partition
        txtfiles = [x for x in os.listdir(webgraph_dir + "/out/pagerank/") if x.startswith("part-")]

        assert len(txtfiles) == 1
        pr_file = webgraph_dir + "/out/pagerank/" + txtfiles[0]
        assert os.path.isfile(pr_file)

        pr = {}
        with open(pr_file, "r") as f:
            for line in f.read().strip().split("\n"):
                pr[line.split(" ")[0].split(".")[0]] = float(line.split(" ")[1])

        assert len(pr) == len(p_graph["graph"])

        for name, data in p_graph["graph"].iteritems():
            maxdiff = p_graph["approx"]
            assert abs(pr[name] - data[0]) <= maxdiff, "PageRank for %s was %s / Expected %s" % (name, pr[name], data[0])

    finally:
        shutil.rmtree(webgraph_dir)
