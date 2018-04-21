from rtypes.pcc.metadata.metadata_base import BuildableMetadata, container
from rtypes.pcc.attributes import staticmethod_predicate, aggregate_property
from rtypes.pcc.utils._utils import ValueParser
from rtypes.pcc.utils.enums import PCCCategories

class SubsetMetadata(BuildableMetadata):
    @property
    def dimension_names(self):
        return [d.name for d in self.dimensions.union(self.group_dimensions)]

    @property
    def is_new_type_predicate(self):
        if self.predicate is None:
            return True
        return isinstance(self.predicate, staticmethod_predicate)

    def __init__(self, cls, parent):
        self.predicate = None
        self.sort_by = None
        self.distinct = None
        self.group_by = None
        self.limit = None
        self.parent = parent
        self.group_dimensions = set()
        self.trigger_dim_strs = set()

        super(SubsetMetadata, self).__init__(cls, PCCCategories.subset)
        self.build_subset_data()
        self.build_required_attrs()
        self.dimension_map = self.rebuild_dimension_map()

    def build_subset_data(self):
        self.group_type = self.parent.group_type
        self.group_members = self.group_type.group_members
        self.group_members.add(self)
        self.immediate_parents = [self.parent]

    def parse_group_dims_as_subset(self):
        group_dimensions = set()
        for attr in dir(self.cls):
            try:
                attr_prop = getattr(self.cls, attr)
            except AttributeError:
                continue
            if isinstance(attr_prop, aggregate_property):
                group_dimensions.add(attr_prop)

        pkey = self.group_type.primarykey
        dims = self.parent.dimensions
        if hasattr(self.parent, "group_dimensions"):
            group_dims = self.parent.group_dimensions.union(group_dimensions)
        else:
            group_dims = group_dimensions
        return pkey, dims, group_dims

    def parse_dimensions(self):
        pkey, dims, group_dims = self.parse_group_dims_as_subset()
        self.primarykey = pkey
        self.dimensions = dims
        self.group_dimensions = group_dims
        if hasattr(self.parent, "namespaces"):
            self.namespaces = self.parent.namespaces
            self.namespace_dimensions = self.parent.namespace_dimensions
            self.namespace_dimensions_str = self.parent.namespace_dimensions_str
        setattr(self.cls, self.primarykey.name, self.primarykey)
        setattr(self.cls, "__primarykey__", self.primarykey)
        for dim in self.dimensions.union(self.group_dimensions):
            setattr(self.cls, dim.name, dim)
        if hasattr(self.cls, "__predicate__"):
            self.predicate = self.cls.__predicate__
            if self.is_new_type_predicate:
                self.trigger_dim_strs.update(
                    set(pdim.name for pdim in self.predicate.dimensions))
        if hasattr(self.cls, "__distinct__"):
            self.distinct = self.cls.__distinct__
            self.categories.add(PCCCategories.impure)
        if hasattr(self.cls, "__limit__"):
            self.limit = self.cls.__limit__
            self.categories.add(PCCCategories.impure)
        if hasattr(self.cls, "__group_by__"):
            self.group_by = self.cls.__group_by__
            self.categories.add(PCCCategories.impure)
        if hasattr(self.cls, "__order_by__"):
            self.sort_by = self.cls.__sort_by__
            self.categories.add(PCCCategories.impure)

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
        final_collection = list()
        if self.group_dimensions:
            final_collection = self.convert_to_grp(
                (item for item in parent_collection
                 if self.run_predicate(item)))
        else:
            final_collection = [
                self.change_type(item)
                for item in parent_collection
                if self.run_predicate(item)]
        if self.sort_by:
            final_collection = sorted(final_collection, key=self.sort_by)
        if self.limit:
            final_collection = final_collection[:self.limit]
        if self.distinct:
            dict_by_prop = dict()
            for item in final_collection:
                if item.__distinct__ not in dict_by_prop:
                    dict_by_prop[item.__distinct__] = item
            final_collection = dict_by_prop.values()

        return final_collection

    def convert_to_grp(self, list_of_objs):
        agg_dict = dict()
        final_result = list()
        for obj in list_of_objs:
            agg_dict.setdefault(
                getattr(obj, self.group_by.name), list()).append(obj)
        for groupkey, objs_for_grp in agg_dict.items():
            obj = container()
            obj.__class__ = self.cls
            obj.__primarykey__ = groupkey
            obj.__group_by__ = groupkey
            for dim in self.group_dimensions:
                setattr(
                    obj, dim.name, dim.on_call_func(
                        [getattr(gobj, dim.target_prop.name)
                         for gobj in objs_for_grp]))
            final_result.append(obj)
        return final_result

    def run_predicate(self, item):
        if not self.is_new_type_predicate:
            return self.predicate(item)
        return self.predicate(*(
            (getattr(item, dim.name)
             if dim.namespace_name is None else
             getattr(getattr(item, dim.namespace_name), dim.name))
            for dim in self.predicate.dimensions))

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
        parent_collection = built_collections[self.parent]
        final_collection = dict()
        if self.group_dimensions:
            # TODO: Build group dimension variation.
            final_collection = dict()
        else:
            final_collection = {
                oid: full_record
                for oid, full_record in parent_collection.iteritems()
                if self.run_predicate_serial(full_record)}
        if self.sort_by:
            final_collection = sorted(
                final_collection,
                key=lambda oid:
                ValueParser.parse(
                    parent_collection[oid]["dims"][self.sort_by.name]))

        if self.limit:
            final_collection = dict(final_collection.items()[:self.limit])

        if self.distinct:
            dict_by_prop = dict()
            for oid in final_collection:
                d_value = ValueParser.parse(
                    parent_collection[oid]["dims"][self.distinct])
                if d_value not in dict_by_prop:
                    dict_by_prop[d_value] = oid
            final_collection = dict_by_prop.values()

        return final_collection

    def run_predicate_serial(self, changes):
        dims = [
            ValueParser.parse(
                changes["dims"][dim.name]
                if dim.namespace_name is None else
                changes[dim.namespace_name][1]["dims"][dim.name])
            for dim in self.predicate.dimensions]
        return self.predicate(*dims)

    def rebuild_dimension_map(self):
        return BuildableMetadata.get_dim_map(
            self.dimensions, self.group_dimensions, set())

    def need_to_check(self, change_tp, dim_changes):
        dims_touched = set(dim_changes.keys())
        return (
            self.parent.need_to_check(change_tp, dim_changes)
            and self.trigger_dim_strs.intersection(dims_touched) != set())

    def check_single_membership(self, change_tp, dim_changes, collection_map):
        value = (
            self.parent.check_single_membership(
                change_tp, dim_changes, collection_map)
            and self.predicate(
                *(ValueParser.parse(dim_changes[d])
                  for d in self.trigger_dim_strs)))
        return value

    def get_base_parents(self):
        return self.parent.get_base_parents()

    def get_parents(self):
        return [self.parent] + self.parent.get_parents()