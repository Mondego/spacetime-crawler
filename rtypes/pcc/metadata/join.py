import uuid
from itertools import product
from rtypes.pcc.metadata.metadata_base import BuildableMetadata, container
from rtypes.pcc.attributes import primarykey, namespace_property
from rtypes.pcc.utils.enums import PCCCategories

@primarykey(str)
def __rtypes__primarykey__(self):
    return self.__rtypes__primarykey

@__rtypes__primarykey__.setter
def __rtypes__primarykey__(self, value):
    self.__rtypes__primarykey = value

class JoinMetadata(BuildableMetadata):
    @property
    def dimension_names(self):
        return [
            d.name for d in self.dimensions.union(self.namespace_dimensions)]

    def __init__(self, cls, parents, namespace_properties, flattened_props):
        self.flattened_props = flattened_props
        self.namespace_dimensions = namespace_properties
        self.namespace_dimensions_str = [
            nprop.__rtypes_property_name__ for nprop in namespace_properties]
        self.namespaces = {
            nprop.__rtypes_property_name__: nprop
            for nprop in namespace_properties}
        super(JoinMetadata, self).__init__(
            cls, PCCCategories.join)
        self.build_join_data(parents)
        self.build_required_attrs()
        self.dimension_map = self.rebuild_dimension_map()

    def build_join_data(self, parents):
        self.group_type = self
        self.group_members = set()
        self.immediate_parents = parents

    def parse_dimensions(self):
        self.primarykey = __rtypes__primarykey__
        setattr(self.cls, self.primarykey.name, self.primarykey)
        setattr(self.cls, "__primarykey__", self.primarykey)

    def build_obj_from_collection(self, collection_map, built_collections=None):
        if built_collections is None:
            # So that same dict is not reused.
            # Problem with default parameters being references in python.
            built_collections = dict()
        parent_collections = list()
        for parent in self.immediate_parents:
            if parent not in built_collections:
                if parent in collection_map:
                    built_collections[parent] = collection_map[parent]
                else:
                    built_collections[parent] = (
                        parent.build_obj_from_collection(
                            collection_map, built_collections))
            if not built_collections[parent]:
                return list()
            parent_collections.append(built_collections[parent])
        return [
            self.setup_join_obj(one_cross)
            for one_cross in product(*parent_collections)]

    def setup_join_obj(self, objs):
        final_obj = self.change_type(container())
        final_obj.__class__ = self.cls
        for nprop, obj in zip(self.namespace_dimensions, objs):
            namespace_obj = nprop.__rtypes_property_namespace_class__()
            namespace_obj.__dict__ = obj.__dict__
            setattr(final_obj, nprop.__rtypes_property_name__, namespace_obj)
        final_obj.__primarykey__ = str(uuid.uuid4())
        return final_obj

    def check_membership_from_serial_collection(
            self, serial_collection_map, built_collections=None):
        if built_collections is None:
                # So that same dict is not reused.
            # Problem with default parameters being references in python.
            built_collections = dict()
        parent_collections = list()
        for parent in self.immediate_parents:
            if parent not in built_collections:
                if parent.name in serial_collection_map:
                    built_collections[parent] = (
                        serial_collection_map[parent.name].iteritems())
                else:
                    built_collections[parent] = (
                        parent.check_membership_from_serial_collection(
                            serial_collection_map, built_collections))
            if not built_collections[parent]:
                return list()
            parent_collections.append(built_collections[parent])
        return {
            str(uuid.uuid4()): dict(
                zip(self.namespace_dimensions_str, one_cross))
            for one_cross in product(*parent_collections)}

    def rebuild_dimension_map(self):
        return BuildableMetadata.get_dim_map(
            self.dimensions, set(), self.namespace_dimensions)

    def need_to_check(self, change_tp, dim_changes):
        return True

    def check_single_membership(self, change_tp, dim_changes, collection_map):
        return True

    def get_base_parents(self):
        result = list()
        for parent in self.immediate_parents:
            result.extend(parent.get_base_parents())
        return result

    def get_parents(self):
        result = list()
        for parent in self.immediate_parents:
            result.extend([parent] + parent.get_parents())
        return result