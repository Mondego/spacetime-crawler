from rtypes.pcc.metadata.metadata_base import Metadata
from rtypes.pcc.utils.enums import PCCCategories
from rtypes.pcc.attributes import rtype_property, aggregate_property, namespace_property


class ProjectionMetadata(Metadata):
    @property
    def dimension_names(self):
        return [
            d.name for d in self.dimensions.union(
                self.namespace_dimensions).union(self.group_dimensions)]

    def __init__(self, cls, parent, projection_dims):
        self.projection_dims = projection_dims
        self.parent = parent
        self.group_dimensions = set()
        self.namespace_dimensions = set()
        self.namespaces = dict()
        super(ProjectionMetadata, self).__init__(
            cls, PCCCategories.projection)
        self.build_projection_data()
        self.build_required_attrs()
        self.dimension_map = self.rebuild_dimension_map()

    def build_projection_data(self):
        self.group_type = self.parent.group_type
        self.group_members = self.group_type.group_members
        self.group_members.add(self)
        self.immediate_parents = [self.parent]

    def parse_group_dims_as_projection(
            self, pkey, dims, group_dims, namespaces):
        if self.group_type.primarykey not in self.projection_dims:
            raise TypeError(
                "Projection class {0} requires "
                "a primary key from the class it projects".format(self.name))

        pkey = self.group_type.primarykey
        dims = set(dim for dim in self.projection_dims
                   if isinstance(dim, rtype_property))
        group_dims = set(dim for dim in self.projection_dims
                         if isinstance(dim, aggregate_property))
        namespaces = set(dim for dim in self.projection_dims
                         if isinstance(dim, namespace_property))
        return pkey, dims, group_dims, namespaces


    def parse_dimensions(self):
        self.primarykey = self.group_type.primarykey
        self.dimensions = set(dim for dim in self.projection_dims
                              if isinstance(dim, rtype_property))
        self.group_dimensions = set(dim for dim in self.projection_dims
                                    if isinstance(dim, aggregate_property))
        # TODO: Namespaces..
        if hasattr(self.parent, "namespaces"):
            self.namespaces = self.parent.namespaces
            self.namespace_dimensions = self.parent.namespace_dimensions
            self.namespace_dimensions_str = self.parent.namespace_dimensions_str
        setattr(self.cls, self.primarykey.name, self.primarykey)
        setattr(self.cls, "__primarykey__", self.primarykey)
        for dim in self.dimensions.union(self.group_dimensions):
            setattr(self.cls, dim.name, dim)

    def build_obj_from_collection(self, collection_map, built_collections=None):
        if built_collections is None:
            # So that same dict is not reused.
            # Problem with default parameters being references in python.
            built_collections = dict()
        if self.parent not in built_collections:
            if self.parent in collection_map:
                built_collections[self.parent] = (
                    collection_map[self.parent])
            else:
                built_collections[self.parent] = (
                    self.parent.build_obj_from_collection(
                        collection_map, built_collections))
        parent_collection = built_collections[self.parent]
        return [
            self.change_type(item)
            for item in parent_collection]

    def check_membership_from_serial_collection(
            self, serial_collection_map, built_collections=None):
        if built_collections is None:
            # So that same dict is not reused.
            # Problem with default parameters being references in python.
            built_collections = dict()
        if self.parent not in built_collections:
            if self.parent.name in serial_collection_map:
                built_collections[self.parent] = (
                    serial_collection_map[self.parent.name])
            else:
                built_collections[self.parent] = (
                    self.parent.check_membership_from_serial_collection(
                        serial_collection_map, built_collections))
        return built_collections[self.parent]

    def rebuild_dimension_map(self):
        return Metadata.get_dim_map(
            self.dimensions, self.group_dimensions, self.namespace_dimensions)

    def need_to_check(self, change_tp, dim_changes):
        return self.parent.need_to_check(change_tp, dim_changes)

    def check_single_membership(self, change_tp, dim_changes, collection_map):
        return self.parent.check_single_membership(
            change_tp, dim_changes, collection_map)

    def get_base_parents(self):
        return self.parent.get_base_parents()

    def get_parents(self):
        return [self.parent] + self.parent.get_parents()