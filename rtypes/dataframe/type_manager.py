#################################################
### Type Management Stuff (Atomic via pause) ####
#################################################
import os
from rtypes.dataframe.dataframe_type import type_lock, DataframeType
from rtypes.pcc.utils.enums import PCCCategories

class TypeManager(object):

    def __init__(self):

        # str name to class.
        self.name2class = dict()

        # set of types that are impure by default
        # and hence cannot be maintained continuously.
        self.impure = set()

        # Types that are only to be read, not written into.
        self.observing_types = set()

        self.tp_to_dataframe_payload = dict()

        self.groupname_to_pure_members = dict()

    #################################################
    ### Static Methods ##############################
    #################################################

    @staticmethod
    def __is_impure(categories):
        return (len(set([PCCCategories.join,
                         PCCCategories.impure,
                         PCCCategories.parameter,
                         PCCCategories.unknown_type]).intersection(
                             categories)) > 0)

    #################################################
    ### API Methods #################################
    #################################################

    def add_type(self, tp, update=None):
        with type_lock:
            pairs_added = self.__add_type(tp, update=update)
        return pairs_added

    def add_types(self, types, update=None):
        pairs_added = set()
        with type_lock:
            for tp in types:
                pairs_added.update(self.__add_type(tp, update=update))
        return pairs_added

    def has_type(self, tp):
        return tp.__rtypes_metadata__.name in self.name2class

    def reload_types(self, types):
        pass

    def remove_type(self, tp):
        pass

    def remove_types(self, types):
        pass

    def check_for_new_insert(self, tp):
        if not hasattr(tp, "__rtypes_metadata__"):
            # Fail to add new obj, because tp was incompatible, or not found.
            raise TypeError(
                "Type %s cannot be inserted/deleted into Dataframe, "
                "declare it as pcc_set." % tp.__class__.__name__)
        metadata = tp.__rtypes_metadata__
        if (metadata.name not in self.name2class
                and metadata != self.name2class[metadata.name]):
            raise TypeError("Type %s hasnt been registered" % metadata.name)
        if metadata not in self.observing_types:
            raise TypeError(
                "Type %s cannot be inserted/deleted into Dataframe, "
                "register it first." % metadata.name)
        if (PCCCategories.pcc_set not in metadata.categories
                and PCCCategories.projection not in metadata.categories):
            # Person not appending the right type of object
            raise TypeError("Cannot insert/delete type %s" % metadata.name)
        if not hasattr(tp, "__primarykey__"):
            raise TypeError(
                "Type must have a primary key dimension "
                "to be used with Dataframes")
        return True

    def check_obj_type_for_insert(self, tp, obj):
        metadata = tp.__rtypes_metadata__
        tp_obj = self.name2class[metadata.name]
        if (metadata.name != obj.__class__.__rtypes_metadata__.name
                and tp_obj.groupname != metadata.name):
            raise TypeError("Object type and type given do not match")
        return True

    def get_requested_type(self, tp):
        return self.get_requested_type_from_str(tp.__rtypes_metadata__.name)

    def get_requested_type_from_str(self, tpname):
        try:
            if tpname in self.name2class:
                return self.name2class[tpname]
            else:
                raise TypeError("Type %s is not registered" % tpname)
        except KeyError:
            raise TypeError("Type %s is not registered" % tpname)

    def get_name2type_map(self):
        return self.name2class

    def get_impures_in_types(self, types, all_types=False):
        if all_types:
            # Passed by reference. very important!!!
            return self.impure
        return set(
            tp for tp in types if self.get_requested_type(tp) in self.impure)

    def get_join_types(self):
        return [
            tp_meta for tp_meta in self.name2class.itervalues()
            if PCCCategories.join in tp_meta.categories]

    def meta_to_pure_members(self, metadata):
        if metadata.groupname in self.groupname_to_pure_members:
            return self.groupname_to_pure_members[metadata.groupname]
        return set()

    def tpname_is_impure(self, tpname):
        return self.metadata_is_impure(self.name2class[tpname])

    def metadata_is_impure(self, tpmeta):
        return self.type_is_impure(tpmeta.cls)

    def type_is_impure(self, tp):
        return tp in self.impure

    def tpname_is_join(self, tpname):
        return self.metadata_is_join(self.name2class[tpname])

    def metadata_is_join(self, meta):
        return PCCCategories.join in meta.categories

    def type_is_join(self, tp):
        return self.metadata_is_join(tp.__rtypes_metadata__)

    #################################################
    ### Private Methods #############################
    #################################################

    def __check_type(self, tp):
        if not hasattr(tp, "__rtypes_metadata__"):
            raise TypeError("Type {0} has to be a PCC Type".format(repr(tp)))

    def __anaylze_metadata(self, metadata, update, observed=True):
        pairs_added = set()
        if metadata.name in self.name2class:
            if observed:
                self.observing_types.add(metadata)
            return pairs_added
        self.name2class[metadata.name] = metadata
        if observed:
            self.observing_types.add(metadata)
        self.groupname_to_pure_members.setdefault(
            metadata.groupname, set()).update(set(
                meta for meta in metadata.group_members
                if (meta.name in self.name2class
                    and not TypeManager.__is_impure(meta.categories))))
        pairs_added.add((
            metadata.name, PCCCategories.pcc_set in metadata.categories))
        group_type = metadata.group_type
        if (group_type is not metadata
                and group_type.name not in self.name2class):
            pairs_added.update(
                self.__anaylze_metadata(group_type, update, observed=False))
        if TypeManager.__is_impure(metadata.categories):
            self.impure.add(metadata.cls)
        self.tp_to_dataframe_payload[metadata] = (
            update
            if not (hasattr(metadata, "group_dimensions")
                    and metadata.group_dimensions) else
            None)
        for parent_meta in metadata.get_parents():
            pairs_added.update(
                self.__anaylze_metadata(parent_meta, update, observed=False))
        return pairs_added

    def __add_type(
            self, tp, update):
        self.__check_type(tp)
        return self.__anaylze_metadata(tp.__rtypes_metadata__, update)
