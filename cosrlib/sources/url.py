from cosrlib.sources import Source
import requests


class UrlSource(Source):
    """ Source that actually fetches an URL. Use with caution! """

    def iter_items(self):

        for url in self.args["urls"]:
            fetched = requests.get(url, headers={'user-agent': 'CommonSearch/dev'})

            if fetched.status_code == 200:
                yield url, fetched.headers, "html", fetched.content
