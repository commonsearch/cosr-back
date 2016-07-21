import importlib
import os
import json
from collections import defaultdict
from cosrlib import re


def load_plugin(path, *args, **kwargs):
    """ Loads and instanciates a Plugin class """

    module_path, cls_name = os.path.splitext(path)

    cls = getattr(importlib.import_module(module_path), cls_name[1:])
    instance = cls(*args, **kwargs)

    return instance


def load_plugins(specs):
    """ Loads and instanciates a list of Plugins from their specs, and returns them indexed by hook """

    hooks = defaultdict(list)

    for spec in (specs or []):
        if spec:
            plugin = load_plugin(*parse_plugin_cli_args(spec))
            for hook in plugin.hooks:
                hooks[hook].append((spec, getattr(plugin, hook)))

    return hooks


def exec_hook(plugins, hook, *args, **kwargs):
    """ Executes one hook on a list of plugins """
    ret = []
    for spec, plugin_hook in plugins[hook]:
        print "Executing hook %s of plugin %s" % (hook, spec)
        ret.append(plugin_hook(*args, **kwargs))

        if ret[-1] == PLUGIN_HOOK_ABORT:
            print "Plugin %s requested abort for hook %s" % (spec, hook)
            break
    return ret


def parse_plugin_cli_args(plugin_spec):
    """ Parses plugin & source arguments:

        --source commoncrawl:limit=1,skip=3
        => ("commoncrawl", {"limit": 1, "skip": 3})

        --source url:http://www.example.com/page
        => ("url", {"url": "http://www.example.com/page"})

        --source 'url:{"url": "http://www.example.com/page"}'
        => ("url", {"url": "http://www.example.com/page"})

    """

    if ":" not in plugin_spec:
        return (plugin_spec, {})

    plugin_name, raw_args = plugin_spec.split(":", 1)

    # JSON
    if re.search(r"^\{", raw_args):
        return (plugin_name, json.loads(raw_args))

    # Named arguments
    elif re.search(r"^[a-zA-Z0-9_]+\=", raw_args):
        args = {}
        for part in raw_args.split(","):
            args[part.split("=")[0]] = part.split("=", 1)[1]

        return (plugin_name, args)

    # Single argument
    else:
        return (plugin_name, {plugin_name: raw_args})


class PLUGIN_HOOK_ABORT(object):
    """ Special value to be returned hen a plugin wants to abort the current hook, thus
        avoiding to run the remaining plugins in this hook
    """
    pass


class Plugin(object):
    """ Base plugin class """

    hooks = frozenset()

    def __init__(self, args):
        self.args = args
        self.init()

    def init(self):
        """ Initialize the plugin """
        pass
