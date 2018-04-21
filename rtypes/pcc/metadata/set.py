from rtypes.pcc.metadata.metadata_base import Metadata
from rtypes.pcc.utils.enums import PCCCategories

class SetMetadata(Metadata):
    @property
    def dimension_names(self):
        return [d.name for d in self.dimensions]

    def __init__(self, cls):
        super(SetMetadata, self).__init__(cls, PCCCategories.pcc_set)
        self.build_set()
        self.build_required_attrs()
        self.dimension_map = self.rebuild_dimension_map()


    def build_set(self):
        pkey, dims = Metadata.get_properties(self.cls)
        self.primarykey = pkey
        self.dimensions = dims
        self.group_type = self

    def build_obj_from_collection(self, collection_map, built_collections=None):
        if self in collection_map:
            return collection_map[self]
        return list()

    def check_membership_from_serial_collection(
            self, serial_collection_map, built_collections=None):
        if self in serial_collection_map:
            return serial_collection_map[self.name]
        return dict()

    def parse_dimensions(self):
        pkey, dims = SetMetadata.get_properties(self.cls)
        self.primarykey = pkey
        self.dimensions = dims
        setattr(self.cls, self.primarykey.name, self.primarykey)
        setattr(self.cls, "__primarykey__", self.primarykey)

    def rebuild_dimension_map(self):
        return super(SetMetadata, self).rebuild_dimension_map()

    def check_single_membership(self, change_tp, dim_changes, collection_map):
        return True

    def get_base_parents(self):
        return [self]

    def get_parents(self):
        return [self]