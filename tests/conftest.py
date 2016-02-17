import pytest
import os
import sys
import logging
import platform

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from cosrlib.urlclient import URLClient
from cosrlib.indexer import Indexer
from cosrlib.ranker import Ranker
from cosrlib.searcher import Searcher


if platform.python_implementation() != "PyPy":
    pytest_plugins = "tests.pytest_profiling"


def pytest_addoption(parser):
    parser.addoption('--repeat', default=1, type='int', metavar='repeat', help='Repeat each test the specified number of times')


def pytest_generate_tests(metafunc):
    for i in range(metafunc.config.option.repeat):
        metafunc.addcall()


class LocalServiceFixture(object):

    def __init__(self, request, options=None):
        self.request = request
        self.options = options or {}

        self.request.addfinalizer(self.stop)

        self.start()

    def start(self):
        self.empty()
        pass

    def stop(self):
        pass

    def empty(self):
        pass


class LocalServiceClientFixture(LocalServiceFixture):

    def start(self):
        self.client = self.make_client()
        self.client.connect()
        self.empty()

    def empty(self):
        self.client.empty()


class IndexerFixture(LocalServiceClientFixture):

    def make_client(self):
        return Indexer()


class RankerFixture(LocalServiceClientFixture):

    def make_client(self):
        return Ranker(self.options["urlclient"].client)


class URLClientFixture(LocalServiceClientFixture):

    def make_client(self):
        return URLClient()


class SearcherFixture(LocalServiceClientFixture):

    def make_client(self):
        return Searcher()


@pytest.fixture(scope="function")
def indexer(request):
    return IndexerFixture(request)


@pytest.fixture(scope="function")
def ranker(request, urlclient):
    return RankerFixture(request, {"urlclient": urlclient})


@pytest.fixture(scope="function")
def urlclient(request):
    return URLClientFixture(request)


@pytest.fixture(scope="function")
def searcher(request):
    return SearcherFixture(request)
