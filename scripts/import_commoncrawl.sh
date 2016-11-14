#!/bin/bash

#
# This scripts imports Common Crawl WARC files and the URL list
# Usage: ./scripts/import_commoncrawl.sh 2
#


# Number of ~1Go WARC files to download. Default is just 1 WARC
WARC_COUNT=${1:-1}

# Common Crawl ID. See http://blog.commoncrawl.org/ for latest dumps
COMMONCRAWL_ID=${COMMONCRAWL_ID:-CC-MAIN-2016-44}

mkdir -p local-data/common-crawl/crawl-data

echo "Downloading file list from Common Crawl: $COMMONCRAWL_ID"
curl 'https://commoncrawl.s3.amazonaws.com/crawl-data/'$COMMONCRAWL_ID'/warc.paths.gz' | gzip -d > local-data/common-crawl/warc.paths.txt

ccfiles="$(cat local-data/common-crawl/warc.paths.txt | head -$WARC_COUNT)"

# Cleanup if there were leftovers from a previous download
find -L local-data/common-crawl/crawl-data -name "*.tmp" | xargs rm -f

for f in ${ccfiles[@]}
do
  if [ -f local-data/$f ]; then
    echo "Already downloaded: `basename $f` ..."
  else
    echo "Downloading: `basename $f` ..."
    echo "---"
    curl --create-dirs https://commoncrawl.s3.amazonaws.com/$f -o local-data/common-crawl/$f.tmp
    mv local-data/common-crawl/$f.tmp local-data/common-crawl/$f
  fi
done

