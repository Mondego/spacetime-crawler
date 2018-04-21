from abc import ABCMeta, abstractmethod, abstractproperty
from rtypes.pcc.attributes import rtype_property
from rtypes.pcc.this import thisclass, thisattr

class container(object):
    pass

class Metadata(object):
    __metaclass__ = ABCMeta

    def __repr__(self):
        return self.name

    @abstractproperty
    def dimension_names(self):
        return [d.name for d in self.dimensions]

    @property
    def groupname(self):
        return self.group_type.name

    def __init__(self, cls, final_category):
        self.name = cls.__module__ + "." + cls.__name__
        self.shortname = cls.__name__
        self.aliases = set([self.name])
        self.cls = cls

        self.group_type = None
        self.categories = set()
        self.group_members = set()

        self.dimensions = set()

        self.primarykey = None

        self.immediate_parents = list()

        self.final_category = final_category
        self.categories.add(final_category)
        self.dimension_map = dict()
        self.parameter_types = dict()

    @staticmethod
    def get_properties(actual_class):
        dimensions = set()
        primarykey = None
        for attr in dir(actual_class):
            try:
                attr_prop = getattr(actual_class, attr)
            except AttributeError:
                continue
            if isinstance(attr_prop, rtype_property):
                dimensions.add(attr_prop)
                if (hasattr(attr_prop, "primarykey")
                        and attr_prop.primarykey != None):
                    if primarykey != None and primarykey != attr_prop:
                        raise TypeError(
                            "Class {0} has more than one primary key".format(
                                repr(actual_class)))
                    primarykey = attr_prop
        return primarykey, dimensions

    @staticmethod
    def get_dim_map(dimensions, group_dimensions, namespace_dimensions):
        dim_map = dict()
        for dim in dimensions:
            dim_map[dim.name] = dim
        for gdim in group_dimensions:
            dim_map[gdim.name] = gdim
        for ndim in namespace_dimensions:
            dim_map[ndim.__rtypes_property_name__] = ndim
        return dim_map

    @abstractmethod
    def parse_dimensions(self):
        pass

    @abstractmethod
    def rebuild_dimension_map(self):
        return Metadata.get_dim_map(self.dimensions, set(), set())

    @abstractmethod
    def build_obj_from_collection(self, collection_map, built_collections=None):
        pass

    @abstractmethod
    def check_membership_from_serial_collection(
            self, serial_collection_map, built_collections=None):
        pass

    @abstractmethod
    def check_single_membership(self, change_tp, dim_changes, collection_map):
        pass

    @abstractmethod
    def get_base_parents(self):
        pass

    def need_to_check(self, change_tp, dim_changes):
        return True

    def build_required_attrs(self):
        self.parse_dimensions()

    def change_type(self, obj):
        new_obj = self.get_dummy_obj()
        new_obj.__dict__ = obj.__dict__
        return new_obj

    def get_dummy_obj(self):
        new_obj = container()
        new_obj.__class__ = self.cls
        return new_obj


class BuildableMetadata(Metadata):
    __metaclass__ = ABCMeta

    @abstractproperty
    def dimension_names(self):
        return [d.name for d in self.dimensions]

    def __init__(self, cls, final_category):
        self.predicate = None
        super(BuildableMetadata, self).__init__(
            cls, final_category)

    @abstractmethod
    def parse_dimensions(self):
        pass

    @abstractmethod
    def rebuild_dimension_map(self):
        return Metadata.get_dim_map(self.dimensions, set(), set())

    @abstractmethod
    def build_obj_from_collection(self, collection_map, built_collections=None):
        pass

    @abstractmethod
    def check_membership_from_serial_collection(
            self, serial_collection_map, built_collections=None):
        pass

    @abstractmethod
    def check_single_membership(self, change_tp, dim_changes, collection_map):
        pass

    @abstractmethod
    def get_base_parents(self):
        pass
