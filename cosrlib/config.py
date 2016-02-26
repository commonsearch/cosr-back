import os
import json
import re

#
# Loads the configuration from (in order of priority):
#  - Default values below (that work well in a local Docker install)
#  - Values in cosr-config.json
#  - COSR_* Environment variables
#

_defaults = {

    # HTTP URL of both ElasticSearch servers
    "ELASTICSEARCHTEXT": "http://192.168.99.100:39200",
    "ELASTICSEARCHDOCS": "http://192.168.99.100:39200",

    # Host:port of the URLserver instance, or "local" for direct import on the same node
    "URLSERVER": "local",  # "192.168.99.100:9702"

    # Host:port of the Explainer instance
    "EXPLAINER": "0.0.0.0:9703",  # "127.0.0.1:9703"

    # Environment type: prod, staging, local, ci, ...
    "ENV": "local",

    # Path to the parent directory of cosrlib
    "PATH_BACK": os.path.dirname(os.path.dirname(__file__))
}

_config_file = os.path.normpath(os.path.join(__file__, "../../cosr-config.json"))
if os.path.isfile(_config_file):
    with open(_config_file, "r") as f:
        _cnt = re.sub(r"\/\*.*?\*\/", "", f.read().replace("\n", ""), flags=re.DOTALL)
        _defaults.update(json.loads(_cnt))

config = {}
for k, default in _defaults.iteritems():
    config[k] = os.getenv("COSR_%s" % k, default)
