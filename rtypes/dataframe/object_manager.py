import datetime
from uuid import uuid4
from dateutil import parser
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary
from rtypes.pcc.types.parameter import ParameterMode

from rtypes.dataframe.dataframe_changes import IDataframeChanges as df_repr
from rtypes.dataframe.dataframe_type import object_lock
from rtypes.pcc.create import create
from rtypes.pcc.utils._utils import ValueParser
from rtypes.pcc.utils.enums import Event, Record, PCCCategories
from rtypes.pcc.triggers import TriggerAction, TriggerTime, BlockAction

class ChangeRecord(object):
    def __init__(
            self, event, tp_obj, oid, dim_change, full_obj,
            fk_type=None, deleted_obj=None):
        self.event = event
        self.tpname = tp_obj.name
        self.groupname = tp_obj.groupname
        self.oid = oid
        self.dim_change = dim_change
        self.full_obj = full_obj
        self.fk_type = fk_type.name if fk_type else None
        self.deleted_obj = deleted_obj
        self.is_projection = PCCCategories.projection in tp_obj.categories

#################################################
#### Object Management Stuff (Atomic Needed) ####
#################################################


class ObjectManager(object):
    def __init__(self, type_manager, bound_execute_trigger_fn):
        # <group key> -> id -> object state.
        # (Might have to make this even better)
        # object state is {"base": base state, "type 1": extra fields etc., ...}
        self.current_state = dict()

        self.object_map = dict()

        self.calculate_pcc = True

        self.deleted_objs = RecursiveDictionary()

        self.type_manager = type_manager

        self.changelog = RecursiveDictionary()

        self.record_obj = RecursiveDictionary()

        self.bound_execute_trigger_fn = bound_execute_trigger_fn

    #################################################
    ### Static Methods ##############################
    #################################################
    @property
    def track_pcc_change_events(self):
        try:
            return self._tpce
        except AttributeError:
            self._tpce = True
            return self._tpce

    @track_pcc_change_events.setter
    def track_pcc_change_events(self, v):
        self._tpce = v

    @property
    def impures_pre_calculated(self):
        try:
            return self._ipc
        except AttributeError:
            self._ipc = False
            return self._ipc

    @impures_pre_calculated.setter
    def impures_pre_calculated(self, v):
        self._ipc = v

    @property
    def propagate_changes(self):
        try:
            return self._pc
        except AttributeError:
            self._pc = True
            return self._pc

    @propagate_changes.setter
    def propagate_changes(self, v):
        self._pc = v

    @property
    def ignore_buffer_changes(self):
        try:
            return self._ibc
        except AttributeError:
            self._ibc = True
            return self._ibc

    @ignore_buffer_changes.setter
    def ignore_buffer_changes(self, v):
        self._ibc = v

    @staticmethod
    def __convert_to_dim_map(obj):
        return RecursiveDictionary(
            (dim, getattr(obj, dim.name))
            for dim in obj.__rtypes_metadata__.dimensions
            if hasattr(obj, dim.name))


    #################################################
    ### API Methods #################################
    #################################################

    def create_table(self, tpname, basetype=False):
        with object_lock:
            return self.__create_table(tpname, basetype)

    def create_tables(self, tpnames_basetype_pairs):
        with object_lock:
            records = list()
            for tpname, basetype in tpnames_basetype_pairs:
                records.extend(self.__create_table(tpname, basetype))
            return records

    def build_pccs(self, pcctypes_objs, universe, params):
        pccs = dict()
        meta_universe = {
            self.type_manager.get_requested_type_from_str(
                tpname): universe[tpname].values()
            for tpname in universe}
        for pcctype_obj in pcctypes_objs:
            pccs[pcctype_obj] = pcctype_obj.build_obj_from_collection(
                meta_universe, pccs)
        return {
            pcctype_obj.cls: {
                obj.__primarykey__: obj
                for obj in pccs.setdefault(pcctype_obj, list())}
            for pcctype_obj in pcctypes_objs}

    def adjust_pcc(self, tp_obj, objs_and_changes):
        if not self.calculate_pcc:
            return list()

        can_be_created_objs = self.type_manager.meta_to_pure_members(tp_obj)
        old_memberships = dict()

        for othertp_obj in can_be_created_objs:
            othertp = othertp_obj.cls
            othertpname = othertp_obj.name
            old_set = old_memberships.setdefault(othertp_obj, set())
            if othertpname in self.object_map:
                old_set.update(
                    set([oid for oid in self.object_map[othertpname]
                         if oid in objs_and_changes]))
        objs = RecursiveDictionary()
        changes = RecursiveDictionary()
        for oid in objs_and_changes:
            objs[oid], changes[oid] = objs_and_changes[oid]

        obj_map = self.build_pccs(
            can_be_created_objs, {tp_obj.groupname: objs}, None)
        records = list()
        for othertp in obj_map:
            othertpname = othertp.__rtypes_metadata__.name
            for oid in obj_map[othertp]:
                event = (Event.Modification
                         if (othertpname in self.object_map
                             and oid in self.object_map[othertpname]) else
                         Event.New)

                try:
                    # Before {either mod or new} - pass
                    if event == Event.Modification: # before update
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.before, TriggerAction.update,
                            None, self.object_map[othertpname][oid],
                            self.object_map[othertpname][oid])
                    else: # Before create
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.before, TriggerAction.create,
                            obj_map[othertp][oid], None, None)
                except BlockAction:
                    pass

                if event == Event.New:
                    # If it is modification, then the modification should
                    # already have been applied when updating the object.
                    # Reassigning the object is going to cause a leak.abs
                    # Leak seen and fixed on 12th Feb 2018.
                    self.object_map.setdefault(
                        othertpname,
                        RecursiveDictionary())[oid] = obj_map[othertp][oid]
                    obj_map[othertp][oid].__rtypes_dataframe_data__ = (
                        self.type_manager.tp_to_dataframe_payload[
                            self.type_manager.get_requested_type(othertp)])
                obj_changes = (
                    ObjectManager.__convert_to_dim_map(obj_map[othertp][oid])
                    if event == Event.New else
                    changes[oid])
                records.extend(
                    self.__create_records(
                        event, self.type_manager.get_requested_type(othertp),
                        oid, obj_changes,
                        ObjectManager.__convert_to_dim_map(
                            self.object_map[othertpname][oid])))

                try:
                    # After {either mod or new} - pass
                    if event == Event.Modification:  # after update
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.after, TriggerAction.update,
                            self.object_map[othertpname][oid], None,
                            self.object_map[othertpname][oid])
                    else: # After create
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.after, TriggerAction.create,
                            self.object_map[othertpname][oid], None,
                            self.object_map[othertpname][oid])
                except BlockAction:
                    pass

        for othertp_obj in old_memberships:
            for oid in old_memberships[othertp_obj].difference(
                    set(obj_map[othertp_obj.cls])):
                if (othertp_obj.name in self.object_map
                        and oid in self.object_map[othertp_obj.name]):
                    records.append(
                        ChangeRecord(
                            Event.Delete, othertp_obj, oid, None, None))

                    # Before - pass
                    try:
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.before, TriggerAction.delete,
                            self.object_map[othertp_obj.name][oid], None, None)
                    except BlockAction:
                        pass

                    self.object_map[othertp_obj.name][oid].__dict__ = dict(
                        self.object_map[othertp_obj.name][oid].__dict__)
                    self.object_map[
                        othertp_obj.name][oid].__start_tracking__ = False

                    obj = self.object_map[othertp_obj.name][oid]
                    del self.object_map[othertp_obj.name][oid]

                    # After - pass
                    try:
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.after, TriggerAction.delete,
                            obj, None, obj)
                    except BlockAction:
                        pass

        return records

    def append(self, tp_obj, obj):
        records = list()
        with object_lock:
            records.extend(self.__append(tp_obj, obj))
            records.extend(
                self.adjust_pcc(tp_obj, {obj.__primarykey__: (obj, None)}))
        return records

    def extend(self, tp_obj, objs):
        records = list()
        obj_map = dict()
        with object_lock:
            for obj in objs:
                records.extend(self.__append(tp_obj, obj))
                obj_map[obj.__primarykey__] = (obj, None)
            records.extend(self.adjust_pcc(tp_obj, obj_map))
        return records

    def get_one(self, tp_obj, oid, parameter):
        obj_map = self.__get(tp_obj, parameter)
        return obj_map[oid] if oid in obj_map else None

    def get(self, tp_obj, parameter):
        return self.__get(tp_obj, parameter).values()

    def delete(self, tp_obj, obj):
        records = []
        oid = obj.__primarykey__
        if (tp_obj.name in self.object_map
                and oid in self.object_map[tp_obj.name]):
            del self.object_map[tp_obj.name][oid]
            if tp_obj.cls == tp_obj.group_type:
                # The object is the group type
                for othertp_obj in tp_obj.pure_group_members:
                    records.extend(self.delete(othertp_obj, obj))

            return [ChangeRecord(Event.Delete, tp_obj, oid, None, None)]
        return records

    def delete_all(self, tp_obj):
        records = []
        if tp_obj.name in self.object_map:
            for obj in self.object_map[tp_obj.name].itervalues():
                records.extend(self.delete(tp_obj, obj))
        return records

    def apply_changes(self, df_changes, except_df=None):
        # see __create_objs function for the format of df_changes
        # if master: send changes to all other dataframes attached.
        # apply changes to object_map, and currect_state
        # adjust pcc
        objs_new, objs_mod, objs_deleted = self.__parse_changes(df_changes)
        records, touched_objs = list(), dict()
        self.__add_new(objs_new, records, touched_objs)
        self.__change_modified(objs_mod, records, touched_objs)
        deletes = self.__delete_marked_objs(objs_deleted, records)
        pcc_adjusted_records = self.__adjust_pcc_touched(touched_objs)
        return records, pcc_adjusted_records, deletes

    def create_records_for_dim_modification(self, tp, oid, dim_change):
        records = self.__create_records(
            Event.Modification, tp.group_type,
            oid, dim_change, None, original_type=tp)
        if tp.group_type != tp and self.track_pcc_change_events:
            records.extend(
                self.__create_records(
                    Event.Modification, tp, oid, dim_change, None))
        return records

    def convert_to_records(self, results, deleted_oids):
        record = list()
        fks = list()
        final_record = RecursiveDictionary()
        for tp in results:
            tp_obj = self.type_manager.get_requested_type(tp)
            for obj in results[tp]:
                fk_part, obj_map = self.__convert_obj_to_change_record(obj)
                fks.extend(fk_part)
                obj_record = final_record.setdefault(
                    tp_obj.groupname, RecursiveDictionary()).setdefault(
                        obj.__primarykey__, RecursiveDictionary())
                obj_record.setdefault(
                    "dims", RecursiveDictionary()).rec_update(obj_map)
                obj_record.setdefault(
                    "types", RecursiveDictionary())[tp_obj.name] = Event.New

        self.__build_fk_into_objmap(fks, final_record)
        for tpname in deleted_oids:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            for oid in deleted_oids[tpname]:
                final_record.setdefault(
                    tp_obj.groupname, RecursiveDictionary()).setdefault(
                        oid, RecursiveDictionary()).setdefault(
                            "types",
                            RecursiveDictionary())[tpname] = Event.Delete
        return final_record

    def convert_whole_object_map(self):
        return self.convert_to_records(
            RecursiveDictionary(
                (self.type_manager.get_requested_type_from_str(tpname).cls,
                 objmap.values())
                for tpname, objmap in self.object_map.items()),
            RecursiveDictionary())

    def add_buffer_changes(self, changes, deletes):
        if self.ignore_buffer_changes:
            return
        try:
            if "gc" not in changes:
                return
            for groupname, group_changes in changes["gc"].items():
                for oid, obj_changes in group_changes.items():
                    for tpname, event in obj_changes["types"].items():
                        try:
                            self.changelog.setdefault(
                                event, RecursiveDictionary()).setdefault(
                                    tpname, RecursiveDictionary())[oid] = (
                                        self.object_map[tpname][oid]
                                        if event != Event.Delete else
                                        deletes[tpname][oid])
                        except Exception:
                            raise
        except Exception:
            return

    def get_new(self, tp):
        metadata = tp.__rtypes_metadata__
        tpname = metadata.name
        return (self.changelog[Event.New][tpname].values()
                if (Event.New in self.changelog
                    and tpname in self.changelog[Event.New]) else
                list())

    def get_mod(self, tp):
        metadata = tp.__rtypes_metadata__
        tpname = metadata.name
        return (self.changelog[Event.Modification][tpname].values()
                if (Event.Modification in self.changelog
                    and tpname in self.changelog[Event.Modification]) else
                list())

    def get_deleted(self, tp):
        metadata = tp.__rtypes_metadata__
        tpname = metadata.name
        return (self.changelog[Event.Delete][tpname].values()
                if (Event.Delete in self.changelog
                    and tpname in self.changelog[Event.Delete]) else
                list())

    def clear_buffer(self):
        self.changelog.clear()

    def clear_all(self):
        for k in self.current_state:
            self.current_state[k].clear()

        for k in self.object_map:
            self.object_map[k].clear()


    #################################################
    ### Private Methods #############################
    #################################################

    def __convert_obj_to_change_record(self, obj):
        fks = list()
        oid = obj.__primarykey__
        dim_change_final = RecursiveDictionary()
        dim_change = self.__convert_to_dim_map(obj)
        for k, v in dim_change.items():
            dim_change_final[k.name] = self.__generate_dim(v, fks, set())
        return fks, dim_change_final

    def __adjust_pcc_touched(self, touched_objs):
        # for eadch tpname, objlist pair in the map, recalculate pccs
        records = list()
        for group_key, changes in touched_objs.items():
            objs_and_changes = RecursiveDictionary()
            for oid, change in changes.items():
                if (group_key in self.object_map
                        and oid in self.object_map[group_key]):
                    objs_and_changes[oid] = (
                        self.object_map[group_key][oid], change)
            records.extend(self.adjust_pcc(
                self.type_manager.get_requested_type_from_str(group_key),
                objs_and_changes))
        return records

    def __delete_marked_objs(self, objs_deleted, records):
        # objs_deleted -> {tp_obj: [oid1, oid2, oid3, ....]}
        # first pass goes through all the base types.
        # Delete base type object, and delete pccs being calculated from that
        # For Eg: If Car is deleted, ActiveCar obj should also be deleted.
        completed_tp = set()
        tpman = self.type_manager
        deletes = RecursiveDictionary()
        for tp_obj in (tp_o
                       for tp_o in objs_deleted
                       if (tp_o.group_type == tp_o
                               and tp_o.groupname in self.object_map)):
            completed_tp.add(tp_obj)
            for oid in objs_deleted[tp_obj]:
                if oid not in self.deleted_objs.setdefault(tp_obj, set()):
                    df_obj = self.object_map[tp_obj.groupname][oid]
                    try:
                        # Before delete trigger - con
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.before, TriggerAction.delete,
                            None, df_obj, df_obj)
                    except BlockAction:
                        continue
                    self.deleted_objs[tp_obj].add(oid)
                    if oid in self.object_map[tp_obj.groupname]:
                        self.object_map[tp_obj.name][oid].__start_tracking__ = (
                            False)
                        if self.propagate_changes:
                            records.append(ChangeRecord(
                                Event.Delete,
                                tp_obj.group_type,
                                oid, None, None,
                                deleted_obj=self.object_map[tp_obj.groupname][
                                    oid]))
                        deletes.setdefault(
                            tp_obj.name, RecursiveDictionary())[oid] = (
                                self.object_map[tp_obj.groupname][oid])
                    # Delete the object
                    del self.object_map[tp_obj.groupname][oid]
                    try:
                        # After delete trigger - pass
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.after, TriggerAction.delete,
                            None, df_obj, None)
                    except BlockAction:
                        pass

                    # Delete any objs related to the one we just deleted
                    for pure_related_pccs_tp in tpman.meta_to_pure_members(
                            tp_obj):
                        if oid in self.object_map[pure_related_pccs_tp.name]:
                            df_obj = (
                                self.object_map[pure_related_pccs_tp.name][oid])
                            try:
                                # Before delete trigger (for related pcc) - pass
                                self.bound_execute_trigger_fn(
                                    tp_obj, TriggerTime.before,
                                    TriggerAction.delete,
                                    None, df_obj, df_obj)
                            except BlockAction:
                                # Can't stop this from happening,
                                # they must be deleted
                                pass

                            self.object_map[
                                pure_related_pccs_tp.name][
                                    oid].__start_tracking__ = False
                            if self.propagate_changes:
                                records.append(ChangeRecord(
                                    Event.Delete,
                                    pure_related_pccs_tp,
                                    oid, None, None,
                                    deleted_obj=self.object_map[
                                        pure_related_pccs_tp.name][oid]))
                            deletes.setdefault(
                                tp_obj.name, RecursiveDictionary())[oid] = (
                                    self.object_map[
                                        pure_related_pccs_tp.name][oid])
                            # Delete the related obj
                            del self.object_map[pure_related_pccs_tp.name][oid]
                            try:
                                # After delete trigger (for related pcc) - pass
                                self.bound_execute_trigger_fn(
                                    tp_obj, TriggerTime.after,
                                    TriggerAction.delete,
                                    None, df_obj, None)
                            except BlockAction:
                                pass
                    del self.current_state[tp_obj.groupname][oid]

        for tp_obj in (tp for tp in objs_deleted if tp not in completed_tp):
            for oid in objs_deleted[tp_obj]:
                if oid not in self.deleted_objs.setdefault(tp_obj, set()):
                    if oid in self.object_map[tp_obj.name]:
                        df_obj = self.current_state[tp_obj.groupname][oid]
                        try:
                            # Before trigger - con
                            self.bound_execute_trigger_fn(
                                tp_obj, TriggerTime.before,
                                TriggerAction.delete,
                                None, df_obj, df_obj)
                        except BlockAction:
                            continue
                        self.object_map[tp_obj.name][oid].__dict__ = dict(
                            self.object_map[tp_obj.name][oid].__dict__)
                        self.object_map[tp_obj.name][oid].__start_tracking__ = (
                            False)
                        if self.propagate_changes:
                            records.append(ChangeRecord(
                                Event.Delete, tp_obj, oid, None, None,
                                deleted_obj=self.object_map[tp_obj.name][oid]))
                        deletes.setdefault(
                            tp_obj.name, RecursiveDictionary())[oid] = (
                                self.object_map[tp_obj.name][oid])
                        del self.object_map[tp_obj.name][oid]
                        try:
                            # After trigger - pass
                            self.bound_execute_trigger_fn(
                                tp_obj, TriggerTime.after, TriggerAction.delete,
                                None, df_obj, None)
                        except BlockAction:
                            pass
                        # Delete the original object as well
                        if len([othertp for othertp in tp_obj.group_members
                                if (othertp.name in self.object_map
                                        and oid in self.object_map[othertp.name]
                                   )]) == 0:
                            del self.current_state[tp_obj.groupname][oid]
                    self.deleted_objs[tp_obj].add(oid)
        return deletes

    def __change_modified(self, objs_mod, records, touched_objs):
        for tp_obj in objs_mod:
            if tp_obj.name not in self.object_map:
                continue
            for oid, obj_and_change in objs_mod[tp_obj].iteritems():
                obj, change = obj_and_change
                if oid not in self.object_map[tp_obj.name]:
                    # Treat as a new object
                    # Not sure what to do.
                    pass
                elif obj != None:
                    df_obj = self.object_map[tp_obj.name][oid]
                    try:
                        # Before trigger
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.before, TriggerAction.update,
                            None, df_obj, df_obj)
                    except BlockAction:
                        continue
                    # Make the update
                    self.object_map[tp_obj.name][oid].__dict__.update(
                        obj.__dict__)
                    try:
                        # After trigger
                        self.bound_execute_trigger_fn(
                            tp_obj, TriggerTime.after, TriggerAction.update,
                            df_obj, None, df_obj)
                    except BlockAction:
                        pass
                touched_objs.setdefault(
                    tp_obj.groupname, RecursiveDictionary())[oid] = change
                if self.propagate_changes:
                    records.extend(
                        self.__create_records(
                            Event.Modification, tp_obj, oid,
                            change, None, True))

    def __add_new(self, objs_new, records, touched_objs):
        for tp_obj in objs_new:
            tp_current_state = self.current_state.setdefault(
                tp_obj.groupname,
                RecursiveDictionary())
            for oid, obj_and_change in objs_new[tp_obj].iteritems():
                obj, change = obj_and_change
                try:
                    self.bound_execute_trigger_fn(
                        tp_obj, TriggerTime.before, TriggerAction.create,
                        obj, None, None)
                except BlockAction:
                    continue
                tp_current_state.setdefault(
                    oid, RecursiveDictionary()).update(obj.__dict__)
                obj.__dict__ = tp_current_state[oid]
                obj.__rtypes_dataframe_data__ = (
                    self.type_manager.tp_to_dataframe_payload[tp_obj])
                obj.__start_tracking__ = True
                if (tp_obj in self.deleted_objs
                        and oid in self.deleted_objs[tp_obj]):
                    self.deleted_objs[tp_obj].remove(oid)
                try:
                    self.bound_execute_trigger_fn(
                        tp_obj, TriggerTime.after, TriggerAction.create,
                        obj, None, None)
                except BlockAction:
                    pass
                self.object_map.setdefault(
                    tp_obj.name, RecursiveDictionary())[oid] = obj
                touched_objs.setdefault(
                    tp_obj.groupname, RecursiveDictionary())[oid] = change
                if self.propagate_changes:
                    records.extend(
                        self.__create_records(
                            Event.New, tp_obj, oid, change, change, True))

    def __parse_changes(self, df_changes):
        '''
        all_changes is a dictionary in this format
        {
            "gc": { <- gc stands for group changes
                "group_key1": { <- Group key for the type EG: Car
                    "object_key1": { <- Primary key of object
                        "dims": { <- optional
                            "dim_name": { <- EG "velocity"
                                "type": <Record type, EG Record.INT.
                                         Enum values can be found in
                                         Record class>
                                "value": <Corresponding value,
                                         either a literal, or a collection,
                                         or a foreign key format.
                                         Can be optional if type is Null
                            },
                            <more dim records> ...
                        },
                        "types": {
                            "type_name": <status of type. Enum values can be
                                         found in Event class>,
                            <More type to status mappings>...
                        }
                    },
                    <More object change logs> ...
                },
                <More group change logs> ...
            },
            "types": [ <- A list of pickled types bein sent for object
                          conversion. Not used atm.
                {
                    "name": <name of the type>,
                    "type_pickled": <pickle string of the type class>
                },
                <More type records> ...
            ]
        }
        '''
        objs_new, objs_mod, objs_deleted = (
            RecursiveDictionary(), RecursiveDictionary(),
            RecursiveDictionary())
        if "gc" not in df_changes:
            df_changes["gc"] = {}
        tm = self.type_manager
        for groupname, group_changes in df_changes["gc"].items():
            try:
                group_type = tm.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            for oid, obj_changes in group_changes.items():
                if groupname in self.deleted_objs and oid in self.deleted_objs:
                    continue

                final_objjson = RecursiveDictionary()
                new_obj = None
                dim_map = RecursiveDictionary()

                # If there are dimension changes to pick up
                if "dims" in obj_changes and len(obj_changes["dims"]) > 0:
                    new_obj, dim_map = self.__build_dimension_obj(
                        obj_changes["dims"],
                        group_type,
                        df_changes["gc"])
                elif (groupname in self.current_state
                      and oid in self.current_state[groupname]):
                    # This is required in case it is an update where type
                    # changes, but the object data is already with the class.
                    new_obj = self.__create_fake_class()()
                    new_obj.__dict__ = self.current_state[groupname][oid]
                else:
                    new_obj = self.__create_fake_class()()

                # For all type and status changes for that object
                for found_member, status in obj_changes["types"].items():
                    types_to_go_through = list()
                    types_to_go_through.append(found_member)
                    # If member is not tracked by the dataframe
                    name2type = tm.get_name2type_map()
                    if not (found_member in name2type
                            and name2type[found_member].groupname == groupname
                            and name2type[found_member] in tm.observing_types):
                        continue
                    found_metadata = tm.get_requested_type_from_str(
                        found_member)
                    # if it is a projection, switch it with the actual type so
                    # that all calculations can be based of that.
                    if PCCCategories.projection in found_metadata.categories:
                        types_to_go_through.append(found_metadata.groupname)

                    for member in types_to_go_through:
                        member_meta = tm.get_requested_type_from_str(
                            member)
                        # If the object is New, or New for this dataframe.
                        if (status == Event.New
                                or status == Event.Modification):
                            if (member not in self.object_map
                                    or oid not in self.object_map[member]):
                                actual_obj = member_meta.change_type(new_obj)
                                objs_new.setdefault(
                                    member_meta, RecursiveDictionary())[oid] = (
                                        actual_obj, obj_changes.setdefault(
                                            "dims", RecursiveDictionary()))
                            # If this dataframe knows this object
                            else:
                                if status == Event.New:
                                    continue
                                # Markin this object as a modified object for
                                # get_mod dataframe call.
                                # Changes to the base object would have already
                                # been applied, or will be applied.
                                objs_mod.setdefault(
                                    member_meta, RecursiveDictionary())[oid] = (
                                        new_obj, obj_changes.setdefault(
                                            "dims", RecursiveDictionary()))
                                # Should get updated through current_state
                                # update when current_state changed.
                            # If the object is being deleted.
                        elif status == Event.Delete:
                            if (member in self.object_map
                                    and oid in self.object_map[member]):
                                # Maintaining a list of deletes for seeing
                                # membership changes later.
                                objs_deleted.setdefault(
                                    member_meta, set()).add(oid)
                        else:
                            raise Exception(
                                "Object change Status %s unknown" % status)
        return objs_new, objs_mod, objs_deleted

    def __create_table(self, tpname, basetype):
        records = list()
        self.object_map.setdefault(tpname, RecursiveDictionary())
        if basetype:
            self.current_state.setdefault(tpname, RecursiveDictionary())
            return records
        else:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            if tp_obj.cls not in self.type_manager.impure:
                obj_map = self.build_pccs(
                    [tp_obj], self.object_map, None)
                self.object_map[tpname] = obj_map[tp_obj.cls]
                records = list()
                for oid in obj_map[tp_obj.cls]:
                    obj_map[tp_obj.cls][oid].__rtypes_dataframe_data__ = (
                        self.type_manager.tp_to_dataframe_payload[tp_obj])
                    obj_changes = ObjectManager.__convert_to_dim_map(
                        obj_map[tp_obj.cls][oid])
                    records.extend(
                        self.__create_records(
                            Event.New, tp_obj,
                            oid, obj_changes,
                            obj_changes))
            return records

    def __append(self, tp_obj, obj):
        records = list()
        tp = tp_obj.cls
        tpname = tp_obj.name
        groupname = tp_obj.groupname
        metadata = tp_obj
        # all clear to insert.
        try:
            oid = obj.__primarykey__
        except AttributeError:
            setattr(obj, tp.__primarykey__.name, str(uuid4()))
            oid = obj.__primarykey__
        tpname = metadata.name
        if oid in self.object_map.setdefault(tpname, RecursiveDictionary()):
            return list()

        # Store the state in records
        self.current_state.setdefault(
            groupname, RecursiveDictionary())[oid] = RecursiveDictionary(
                obj.__dict__)

        # Set the object state by reference
        # to the original object's symbol table
        obj.__dict__ = self.current_state[groupname][oid]
        obj.__rtypes_dataframe_data__ = (
            self.type_manager.tp_to_dataframe_payload[tp_obj])
        self.object_map.setdefault(tpname, RecursiveDictionary())[oid] = obj
        self.object_map[tpname][oid].__start_tracking__ = True
        obj_changes = ObjectManager.__convert_to_dim_map(obj)
        records.extend(
            self.__create_records(
                Event.New, tp_obj, oid, obj_changes, obj_changes))
        return records

    def __get(self, tp_obj, parameter):
        tp = tp_obj.cls
        tpname = tp_obj.name
        with object_lock:
            if (tp not in self.type_manager.impure
                    or self.impures_pre_calculated):
                return (self.object_map[tpname]
                        if tpname in self.object_map else
                        dict())
            obj_map = self.build_pccs(
                [tp_obj], self.object_map, parameter)
            return obj_map[tp] if tp in obj_map else dict()

    def __create_records(
            self, event, tp_obj, oid, obj_changes, full_obj_map,
            converted=False, fk_type_to=None, original_type=None):
        if event == Event.Delete:
            if (tp_obj.groupname in self.record_obj
                    and oid in self.record_obj[tp_obj.groupname]):
                del self.record_obj[tp_obj.groupname][oid]
        elif event == Event.New:
            self.record_obj.setdefault(
                tp_obj.groupname, RecursiveDictionary())[oid] = (
                    RecursiveDictionary(full_obj_map))
        elif event == Event.Modification:
            if (tp_obj.groupname in self.record_obj
                    and oid in self.record_obj[tp_obj.groupname]
                    and obj_changes):
                self.record_obj[tp_obj.groupname][oid].update(obj_changes)
            else:
                if full_obj_map == None:
                    try:
                        full_obj_map = ObjectManager.__convert_to_dim_map(
                            self.object_map[tp_obj.name][oid])
                    except:
                        if original_type:
                            full_obj_map = ObjectManager.__convert_to_dim_map(
                                self.object_map[original_type.name][oid])
                        else:
                            raise TypeError(
                                "Unknown error. Trying to modify an object "
                                "that is weirdly tracked by dataframe? It is "
                                "tracked by the dataframe, but is not in the "
                                "database")
                self.record_obj.setdefault(
                    tp_obj.groupname, RecursiveDictionary()).setdefault(
                        oid, RecursiveDictionary()).update(full_obj_map)

        if not full_obj_map and event != Event.Delete and not converted:
            full_obj_map = self.record_obj[tp_obj.groupname][oid]
        records = list()
        fks = list()
        new_obj_changes = RecursiveDictionary()
        new_full_obj_map = RecursiveDictionary()
        if converted:
            records.append(
                ChangeRecord(
                    event, tp_obj, oid, obj_changes, full_obj_map, fk_type_to))
            if obj_changes:
                for k, v in obj_changes.items():
                    if v["type"] == Record.FOREIGN_KEY:
                        fk = v["value"]["object_key"]
                        fk_event = (
                            Event.Modification
                            if (v["value"]["group_key"] in self.object_map
                                and fk in self.object_map[
                                    v["value"]["group_key"]]) else
                            Event.New)
                        fk_type_obj = (
                            self.type_manager.get_requested_type_from_str(
                                v["value"]["actual_type"]["name"]))
                        fk_full_obj = self.__convert_to_dim_map(
                            self.object_map[fk_type_obj.groupname][fk])
                        if (fk_event == Event.New
                                and fk_type_obj.groupname in self.object_map
                                and fk in self.object_map[
                                    fk_type_obj.groupname]):
                            fk_dims = fk_full_obj
                        records.extend(
                            self.__create_records(
                                fk_event, fk_type_obj, fk, fk_dims,
                                fk_full_obj, fk_type_to=tp_obj))
            if full_obj_map:
                for k, v in full_obj_map.items():
                    if v["type"] == Record.FOREIGN_KEY:
                        fk = v["value"]["object_key"]
                        fk_event = (
                            Event.Modification if (
                                v["value"]["group_key"] in self.object_map
                                and fk in self.object_map[
                                    v["value"]["group_key"]]) else
                            Event.New)
                        fk_type_obj = (
                            self.type_manager.get_requested_type_from_str(
                                v["value"]["actual_type"]["name"]))
                        fk_full_obj = self.__convert_to_dim_map(
                            self.object_map[fk_type_obj.groupname][fk])
                        if (fk_event == Event.New
                                and fk_type_obj.groupname in self.object_map
                                and fk in self.object_map[
                                    fk_type_obj.groupname]):
                            fk_dims = fk_full_obj
                        records.extend(
                            self.__create_records(
                                fk_event, fk_type_obj, fk, fk_dims,
                                fk_full_obj, fk_type_to=tp_obj))
            return records

        if obj_changes:
            for k, v in obj_changes.items():
                if not hasattr(k, "name"):
                    new_obj_changes[k] = v
                else:
                    new_obj_changes[k.name] = self.__generate_dim(
                        v, fks, set())
        if full_obj_map:
            if full_obj_map == obj_changes:
                new_full_obj_map = new_obj_changes
            else:
                for k, v in full_obj_map.items():
                    if type(k) == str:
                        new_full_obj_map[k] = v
                    else:
                        new_full_obj_map[k.name] = self.__generate_dim(
                            v, fks, set())
        for fk, fk_type_obj, fk_obj in fks:
            group = fk_type_obj.groupname
            fk_event_type = (
                Event.Modification if (
                    group in self.object_map
                    and fk in self.object_map[group]) else
                Event.New)
            fk_dims = None
            fk_full_obj = self.__convert_to_dim_map(fk_obj)
            if (fk_event_type == Event.New
                    and group in self.object_map
                    and fk in self.object_map[group]):
                fk_dims = fk_full_obj
            records.extend(
                self.__create_records(
                    fk_event_type, fk_type_obj, fk, fk_dims,
                    fk_full_obj, fk_type_to=tp_obj))
        records.append(
            ChangeRecord(
                event, tp_obj, oid, new_obj_changes,
                new_full_obj_map, fk_type_to))
        return records

    def __build_dimension_obj(self, dim_received, group_obj, full_record):
        groupname = group_obj.name
        dim_map = RecursiveDictionary()
        obj = group_obj.get_dummy_obj()
        for dim in dim_received:
            record = dim_received[dim]
            dim_map[dim] = record
            if not hasattr(group_obj.cls, dim):
                continue
            if record["type"] == Record.OBJECT:
                new_record = RecursiveDictionary()
                new_record["type"] = Record.DICTIONARY
                new_record["value"] = record["value"]["omap"]
                dict_value = self.__process_record(new_record, full_record)
                value = self.__create_fake_class()()
                value.__dict__ = dict_value
                value.__class__ = getattr(group_obj.cls, dim).type
            elif (record["type"] == Record.COLLECTION
                  or record["type"] == Record.DICTIONARY):
                collect = self.__process_record(record, full_record)
                value = getattr(group_obj.cls, dim).type(collect)
            else:
                value = self.__process_record(record, full_record)
            setattr(obj, dim, value)
        return obj, dim_map

    def __process_record(self, record, full_record):
        tm = self.type_manager
        if record["type"] == Record.INT:
            # the value will be in record["value"]
            return long(record["value"])
        if record["type"] == Record.FLOAT:
            # the value will be in record["value"]
            return float(record["value"])
        if record["type"] == Record.STRING:
            # the value will be in record["value"]
            return record["value"]
        if record["type"] == Record.BOOL:
            # the value will be in record["value"]
            return record["value"]
        if record["type"] == Record.NULL:
            # No value, just make it None
            return None

        if record["type"] == Record.OBJECT:
            # The value is {
            #    "omap": <Dictionary Record form of the object (__dict__)>,
            #    "type": {"name": <name of type,
            #             "type_pickled": pickled type <- optional part
            #  }

            # So parse it like a dict and update the object dict
            new_record = RecursiveDictionary()
            new_record["type"] = Record.DICTIONARY
            new_record["value"] = record["value"]["omap"]

            dict_value = self.__process_record(new_record, full_record)
            value = self.__create_fake_class()()
            # Set type of object from record.value.object.type. Future work.
            value.__dict__ = dict_value
            return value
        if record["type"] == Record.COLLECTION:
            # Assume it is list, as again, don't know this type
            # value is just list of records
            return [
                self.__process_record(rec, full_record)
                for rec in record["value"]]
        if record["type"] == Record.DICTIONARY:
            # Assume it is dictionary, as again, don't know this type
            # value-> [{"k": key_record, "v": val_record}]
            # Has to be a list because keys may not be string.
            return RecursiveDictionary([
                (self.__process_record(p["k"], full_record),
                 self.__process_record(p["v"], full_record))
                for p in record["value"]])
        if record["type"] == Record.DATETIME:
            return parser.parse(record["value"])
        if record["type"] == Record.FOREIGN_KEY:
            # value -> {"group_key": group key,
            #           "actual_type": {"name": type name,
            #                           "type_pickled": pickled type},
            #           "object_key": object key}
            groupname = record["value"]["group_key"]
            oid = record["value"]["object_key"]
            name2type = tm.get_name2type_map()
            if groupname not in name2type:
                # This type cannot be created,
                # it is not registered with the DataframeModes.
                return None
            actual_type_name = (
                record["value"]["actual_type"]["name"]
                if ("actual_type" in record["value"]
                    and "name" in record["value"]["actual_type"]) else
                groupname)
            actual_type_name, actual_type = (
                (actual_type_name,
                 tm.get_name2type_map()[actual_type_name].cls)
                if (actual_type_name in tm.get_name2type_map()) else
                (groupname,
                 tm.get_name2type_map()[groupname].cls))

            if (groupname in self.current_state
                    and oid in self.current_state[groupname]):
            # The object exists in one form or the other.
                if (actual_type_name in self.object_map
                        and oid in self.object_map[actual_type_name]):
                    # If the object already exists.
                    # Any new object will update that.
                    return self.object_map[actual_type_name][oid]
                # The group object exists, but not in the actual_type obj.
            # The object does not exist, create a dummy one and the actual
            # object will get updated
            # in some other group change in this iteration.
            if (groupname in full_record
                    and oid in full_record[groupname]
                    and actual_type_name in full_record[groupname][oid]["types"]
                    and full_record[groupname][
                        oid]["types"][actual_type_name] == Event.New):
                # Object is in the incoming record. Can build that.
                # Duplicates will not be built anyway.
                obj, _ = self.__build_dimension_obj(
                    full_record[groupname][oid]["dims"],
                    tm.get_requested_type_from_str(groupname),
                    full_record)
                obj_state = self.current_state.setdefault(
                    groupname, RecursiveDictionary()).setdefault(
                        oid, RecursiveDictionary())
                obj_state.update(obj.__dict__)
                obj.__dict__ = obj_state
                obj.__class__ = actual_type
                self.object_map.setdefault(
                    actual_type_name, RecursiveDictionary())[oid] = obj
                return obj

        raise TypeError("Do not know dimension type %s", record["type"])

    def __generate_dim(self, dim_change, foreign_keys, built_objs):
        try:
            if dim_change in built_objs:
                raise RuntimeError(
                    "Cyclic reference in the object to be serialized. %s",
                    dim_change)
        except TypeError:
            pass
        dim_type = ValueParser.get_obj_type(dim_change)
        dim = RecursiveDictionary()
        dim["type"] = dim_type
        if dim_type == Record.INT:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.FLOAT:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.STRING:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.BOOL:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.NULL:
            return dim

        if dim_type == Record.COLLECTION:
            dim["value"] = [self.__generate_dim(
                v, foreign_keys, built_objs) for v in dim_change]
            return dim

        if dim_type == Record.DICTIONARY:
            dim["value"] = [RecursiveDictionary(
                {"k": self.__generate_dim(k, foreign_keys, built_objs),
                 "v": self.__generate_dim(v, foreign_keys, built_objs)})
                for k, v in dim_change.items()]
            return dim

        if dim_type == Record.OBJECT:
            try:
                built_objs.add(dim_change)
            except TypeError:
                pass
            dim["value"] = RecursiveDictionary()
            dim["value"]["omap"] = (
                self.__generate_dim(
                    dim_change.__dict__, foreign_keys, built_objs)["value"])
            # Can also set the type of the object here serialized. Future work.
            return dim

        if dim_type == Record.FOREIGN_KEY:
            key = self.type_manager.get_requested_type(dim_change.__class__)
            convert_type = key.group_type
            foreign_keys.append((key, convert_type, dim_change))
            dim["value"] = RecursiveDictionary()
            dim["value"]["group_key"] = convert_type.groupname
            dim["value"]["object_key"] = key
            dim["value"]["actual_type"] = RecursiveDictionary()
            dim["value"]["actual_type"]["name"] = convert_type.name
            return dim

        if dim_type == Record.DATETIME:
            dim["value"] = "%d-%d-%d" % (
                dim_change.year, dim_change.month, dim_change.day)
            return dim

        raise TypeError("Don't know how to deal with %s" % dim_change)

    def __create_fake_class(self):
        class container(object):
            pass
        return container

    def __build_fk_into_objmap(self, fks, final_record):
        if len(fks) == 0:
            return
        more_fks = list()
        for fk, fk_type_obj, fk_obj in fks:
            group = fk_type_obj.groupname
            fk_event_type = Event.New
            new_fks, fk_full_obj = self.__convert_obj_to_change_record(fk_obj)
            more_fks.extend(new_fks)
            fk_obj_record = final_record.setdefault(
                fk_type_obj.groupname, RecursiveDictionary()).setdefault(
                    fk, RecursiveDictionary())
            fk_obj_record.setdefault(
                "dims", RecursiveDictionary()).update(fk_full_obj)
            fk_obj_record.setdefault(
                "types", RecursiveDictionary())[fk_type_obj.name] = Event.New
        self.__build_fk_into_objmap(more_fks, final_record)
