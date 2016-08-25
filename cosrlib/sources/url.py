from cosrlib.sources import Source
import requests


class UrlSource(Source):
    """ Source that actually fetches an URL. Use with caution! """

    def get_partitions(self):

        if self.args.get("urls"):
            return [{
                "url": url
            } for url in self.args["urls"]]
        else:
            return [{
                "url": self.args["url"]
            }]

    def iter_items(self, partition):

        fetched = requests.get(partition["url"], headers={'user-agent': 'CommonSearch/dev'})

        if fetched.status_code == 200:
            yield partition["url"], fetched.headers, "html", 2, fetched.content
