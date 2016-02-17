import importlib


def load_signal(signal_name, **kwargs):
    """ Loads and instanciates a ranking signal class """

    cls_name = "Signal"
    cls = getattr(importlib.import_module("cosrlib.signals.%s" % signal_name), cls_name)
    instance = cls(**kwargs)

    return instance


class BaseSignal(object):
    """ Base ranking signal class, returning floats or dicts """

    # Does this signal use data from the urlserver?
    uses_url_metadata = True

    def get_value(self, document, url_metadata):
        raise NotImplementedError
