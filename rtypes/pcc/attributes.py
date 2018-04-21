'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
import uuid
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary
from abc import ABCMeta, abstractmethod

def get_type(obj):
    # both iteratable/dictionary + object type is messed up. Won't work.
    try:
        if hasattr(obj, "__dependent_type__"):
            return "dependent"
        if dict in type(obj).mro():
            return "dictionary"
        if hasattr(obj, "__iter__"):
            return "collection"
        if len(set([float, int, str, unicode, type(None)]).intersection(
                set(type(obj).mro()))) > 0:
            return "primitive"
        if hasattr(obj, "__dict__"):
            return "object"
    except TypeError as e:
        return "unknown"
    return "unknown"

class rtype_property(property):

    def __repr__(self):
        return self.name

    def __hash__(self):
        return hash(
            (self.type, self.dimension, self.name, self.primarykey))

    def __init__(self, tp, fget, fset=None, fdel=None, doc=None):
        self.type = tp
        self.dimension = True
        self.name = fget.func_name
        self.primarykey = None
        self.namespace_name = None

        # the next one is only for dataframe use
        self.__rtypes_dataframe_data__ = set()
        property.__init__(self, fget, fset, fdel, doc)

    def setter(self, fset):
        prop = rtype_property(self.type, self.fget, fset)
        for att in self.__dict__:
            setattr(prop, att, self.__dict__[att])
        return prop

    def get_namespace_version(self, parent_name):
        prop = rtype_property(self.type, self.fget, self.fset)
        for att in self.__dict__:
            setattr(prop, att, self.__dict__[att])
        prop.namespace_name = parent_name
        return prop

    def __copy__(self):
        prop = property(self.fget, self.fset)
        prop.__dict__.update(self.__dict__)
        return prop

    def update(self, obj, value):
        property.__set__(self, obj, value)

    def __set__(self, obj, value, bypass=False):
        #if not hasattr(obj, "__start_tracking__"):
        #    return
        # Dataframe is present
        if (hasattr(obj, "__rtypes_dataframe_data__")
                and obj.__rtypes_dataframe_data__
                and hasattr(obj, "__primarykey__")
                and obj.__primarykey__):
            # Get dataframe method from the payload
            dataframe_update_method = obj.__rtypes_dataframe_data__
            # execute this method in the dataframe
            dataframe_update_method(self, obj, value)
        # no dataframe
        else:
            self.update(obj, value)


class primarykey(object):
    def __init__(self, tp=None, default=True):
        self.type = tp if tp else "primitive"
        self.default = default

    def __call__(self, func):
        rprop = rtype_property(self.type, func)
        rprop.primarykey = True
        return rprop


class dimension(object):
    def __init__(self, tp=None):
        self.type = tp if tp else "primitive"

    def __call__(self, func):
        return rtype_property(self.type, func)


class aggregate_property(property):
    def __init__(
            self, prop, on_call_func,
            fget=None, fset=None, fdel=None, doc=None):
        self.name = fget.func_name
        self.target_prop = prop
        self.on_call_func = on_call_func
        property.__init__(self, fget, fset, fdel, doc)

    def setter(self, fset):
        prop = aggregate_property(
            self.target_prop, self.on_call_func, self.fget, fset)
        for a in self.__dict__:
            setattr(prop, a, self.__dict__[a])
        return prop


class aggregate(object):
    __metaclass__ = ABCMeta
    def __init__(self, prop):
        self.prop = prop
        if not isinstance(prop, rtype_property):
            raise TypeError("Cannot create aggregate type with given property")

    def __call__(self, func):
        return aggregate_property(self.prop, self.on_call, func)

    @abstractmethod
    def on_call(self, list_of_target_prop):
        raise NotImplementedError(
            "Abstract class implementation. Not to be called.")


class summation(aggregate):
    def on_call(self, list_of_target_prop):
        return sum(list_of_target_prop)


class count(aggregate):
    def on_call(self, list_of_target_prop):
        return len(list_of_target_prop)


class average(aggregate):
    def on_call(self, list_of_target_prop):
        return float(
            sum(list_of_target_prop)) / float(len(list_of_target_prop))


class maximum(aggregate):
    def on_call(self, list_of_target_prop):
        return max(list_of_target_prop)


class minimum(aggregate):
    def on_call(self, list_of_target_prop):
        return min(list_of_target_prop)

class namespace_property(object):
    def __init__(self, name, tp):
        self.__rtypes_property_name__ = name
        self.__rtypes_property_type__ = tp
        dim_names = (
            self.__rtypes_property_type__.__rtypes_metadata__.dimension_names)
        self.__rtypes_property_dimensions__ = set()
        for dname in dim_names:
            prop = getattr(
                self.__rtypes_property_type__,
                dname).get_namespace_version(name)
            setattr(self, dname, prop)
            self.__rtypes_property_dimensions__.add(prop)
        self.__rtypes_property_namespace_class__ = self.get_container()

    def get_container(self):
        class container(object):
            pass

        for dim in self.__rtypes_property_dimensions__:
            setattr(container, dim.name, dim)
        return container


class staticmethod_predicate(object):
    def __init__(self, func, dimensions):
        self.dimensions = dimensions
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class predicate(object):
    def __init__(self, *dimensions):
        self.dimensions = dimensions

    def __call__(self, func):
        return staticmethod_predicate(func, self.dimensions)
