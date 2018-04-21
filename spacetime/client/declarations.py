'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from spacetime.common.modes import Modes

class DataAgent(object):
    def __init__(self, keywords):
        if "host" in keywords:
            self.host = keywords.rstrip("/") + "/"
        else:
            self.host = "default"

    def __call__(self, actual_class):
        if actual_class.__special_wire_format__ is None:
            actual_class.__special_wire_format__ = dict()
        return actual_class


class Producer(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.Producing] = self.types
        return DataAgent.__call__(self, actual_class)

class Tracker(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.Tracker] = self.types
        return DataAgent.__call__(self, actual_class)

class Getter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.Getter] = self.types
        return DataAgent.__call__(self, actual_class)

class GetterSetter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.GetterSetter] = self.types
        return DataAgent.__call__(self, actual_class)

class Deleter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.Deleter] = self.types
        return DataAgent.__call__(self, actual_class)

class Setter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.Setter] = self.types
        return DataAgent.__call__(self, actual_class)

class ServerTriggers(DataAgent):
    def __init__(self, *functions, **keywords):
        self.functions = functions
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.functions:
            if actual_class.__declaration_map__ is None:
                actual_class.__declaration_map__ = dict()
            actual_class.__declaration_map__.setdefault(
                self.host, dict())[Modes.Triggers] = self.functions
        return DataAgent.__call__(self, actual_class)
