from cosrlib.sources import Source
import requests


class UrlSource(Source):
    """ Source that actually fetches an URL. Use with caution! """

    def get_partitions(self):

        if self.args.get("urls"):
            return self.args["urls"]
        else:
            return [self.args["url"]]

    def iter_items(self, partition):

        url = partition

        fetched = requests.get(url, headers={'user-agent': 'CommonSearch/dev'})

        if fetched.status_code == 200:
            yield url, fetched.headers, "html", 2, fetched.content
