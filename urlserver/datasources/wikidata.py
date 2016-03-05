from . import BaseDataSource
from cosrlib.url import URL


class DataSource(BaseDataSource):
    """ Return the title and description from Wikidata, when present in "official website"
    """

    dump_testdata = "tests/testdata/wikidata.json"
    dump_url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz"
    dump_compression = "gz"
    dump_format = "json-lines"
    dump_batch_size = 10000

    def import_row(self, i, row):
        """ Maps a raw data row into a list of (key, values) pairs """

        # https://www.wikidata.org/wiki/Property:P856
        official_website = None
        for claim in row["claims"].get("P856", []):
            if (
                    claim["type"] == "statement" and
                    claim["mainsnak"]["datatype"] == "url" and
                    claim["mainsnak"].get("datavalue")
            ):
                official_website = URL(claim["mainsnak"]["datavalue"]["value"]).normalized

        # TODO: other languages!
        label_en = row["labels"].get("en", {}).get("value") or ""
        description_en = row["descriptions"].get("en", {}).get("value") or ""

        if official_website:
            yield official_website, {
                "wikidata_title": label_en,
                "wikidata_description": description_en
            }
