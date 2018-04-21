class UnionOrIntersectionMetadata(Metadata):
    __metaclass__ = ABCMeta

    def __init__(self, cls, final_category, parents, projection_dims=None):
        super(UnionOrIntersectionMetadata, self).__init__(
            cls, final_category, parents, projection_dims=projection_dims)
        self.build_union_or_intersection_data()

    def build_union_or_intersection_data(self):
        pass

    def parse_group_dims_as_union_or_intersection(
            self, pkey, dims, group_dims, namespaces):
        if len(set(p.primarykey for p in self.parents)) > 1:
            raise TypeError(
                "Union class {0} requires that all participating classes have "
                "the same name for the primary key".format(self.name))

        pkey = self.parents[0].primarykey
        dims = set()
        group_dims = set()
        namespaces = dict()
        for p in self.parents:
            if dims:
                dims.intersection_update(p.dimensions)
            else:
                dims = set(p.dimensions)
            if group_dims:
                group_dims.intersection_update(p.group_dimensions)
            else:
                group_dims = set(p.group_dimensions)
            for ns in p.namespaces:
                namespaces.setdefault(
                    ns, p.namespaces[ns]).intersection_update(
                        p.namespaces[ns])

        return pkey, dims, group_dims, namespaces

    @abstractmethod
    def build_obj_from_collection(self, collection_map):
        pass

    @abstractmethod
    def build_obj_from_serial_collection(self, serial_collection_map):
        pass


class UnionMetadata(UnionOrIntersectionMetadata):
    def build_obj_from_collection(self, collection_map):
        pass

    def build_obj_from_serial_collection(self, serial_collection_map):
        pass


class IntersectionMetadata(UnionOrIntersectionMetadata):
    def build_obj_from_collection(self, collection_map):
        pass

    def build_obj_from_serial_collection(self, serial_collection_map):
        pass


