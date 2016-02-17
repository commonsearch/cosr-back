#!/usr/bin/env python

#
# This scripts empties and recreate the Elasticsearch indexes
#


import sys
import os
sys.path.insert(-1, os.getcwd())

from cosrlib.indexer import Indexer

indexer = Indexer()


if "--delete" in sys.argv or (raw_input("Do you want to delete the current indices and all data? [y/N]") == "y"):

  indexer.empty()

  print "Reset done."
