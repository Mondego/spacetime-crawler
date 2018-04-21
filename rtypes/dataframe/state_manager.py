import time
import uuid
from rtypes.dataframe.type_manager import TypeManager
from rtypes.pcc.utils.enums import PCCCategories
from rtypes.dataframe.dataframe_type import object_lock
from rtypes.pcc.utils._utils import ValueParser
from rtypes.pcc.utils.enums import Event, Record
from rtypes.dataframe.state_recorder import StateRecorder


#################################################
#### Object Management Stuff (Atomic Needed) ####
#################################################


class StateManager(object):
    def __init__(self, maintain_change_record):
        # A map of the form
        # self.type_to_obj_dimstate = {
        #     group_key1 : {
        #         oid1 : RecursiveDictionary({  RecursiveDictionary is ordered.
        #             timestamp1 : {
        #                 "dims": <dimension changes>
        #             },
        #             timestamp2: {
        #                 "dims": <dimension changes>
        #             }, ... More timestamps
        #         }), ... More objects
        #     }, ... More groups
        # }
        self.type_to_obj_dimstate = dict()

        # self.type_to_obj_objids = {
        #     tpname1: set([oid1, oid2, ...]),
        #     tpname2: set([...]), ...}
        self.type_to_objids = dict()
        self.type_manager = TypeManager()
        self.impure_type_to_objids = dict()
        self.join_ids = dict()
        self.maintain = maintain_change_record

    #################################################
    ### Static Methods ##############################
    #################################################


    #################################################
    ### API Methods #################################
    #################################################

    def add_types(self, types):
        pairs = self.type_manager.add_types(types)
        self.create_tables(pairs)

    def add_type(self, tp):
        pairs = self.type_manager.add_type(tp)
        self.create_tables(pairs)

    def create_tables(self, tpnames_basetype_pairs):
        with object_lock:
            for tpname, is_set in tpnames_basetype_pairs:
                self.__create_table(tpname, is_set)

    def apply_changes(self, df_changes, except_app):
        if "gc" in df_changes:
            self.__apply_changes(df_changes["gc"], except_app)

    def clear_all(self):
        for tpname in self.type_to_objids:
            if tpname in self.type_to_obj_dimstate:
                self.type_to_obj_dimstate[tpname] = StateRecorder(
                    tpname, self.maintain)
            self.type_to_objids[tpname].clear()

    def get_records(self, changelist, app):
        final_record = dict()
        pcc_types_to_process = set()
        impure_pccs_to_process = set()
        for tpname in changelist:
            if tpname in self.type_to_obj_dimstate:
                # It is a base type. Can pull dim state for it directly.
                new_oids, mod_oids, del_oids = (
                    self.__get_oid_change_buckets(tpname, changelist[tpname]))
                tp_record = (
                    self.__get_dim_changes_for_basetype(
                        tpname, changelist[tpname],
                        new_oids, mod_oids, del_oids, app))
                if tp_record:
                    final_record[tpname] = tp_record
                    self.__set_type_change_status(
                        tpname, new_oids, mod_oids, del_oids,
                        final_record[tpname])
            elif self.type_manager.tpname_is_impure(tpname):
                impure_pccs_to_process.add(tpname)
            else:
                pcc_types_to_process.add(tpname)

        impure_pccs = dict()
        for tpname in impure_pccs_to_process:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            if tp_obj in impure_pccs:
                continue
            impure_pccs[tp_obj] = (
                tp_obj.check_membership_from_serial_collection(
                    self.type_to_obj_dimstate, built_collections=impure_pccs))
        impure_gpchanges = dict()
        for tpname in impure_pccs_to_process:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            if tp_obj not in impure_pccs or not impure_pccs[tp_obj]:
                continue
            if self.type_manager.metadata_is_join(tp_obj):
                for oid in impure_pccs[tp_obj]:
                    noids, moids, doids = self.__setup_join(
                        oid, impure_pccs[tp_obj][oid], tp_obj, final_record,
                        changelist)
                    for gpobj in noids:
                        impure_gpchanges.setdefault(
                            gpobj, {
                                "new": set(),
                                "mod": set(),
                                "del": set()})["new"].update(noids[gpobj])
                    for gpobj in moids:
                        impure_gpchanges.setdefault(
                            gpobj, {
                                "new": set(),
                                "mod": set(),
                                "del": set()})["mod"].update(moids[gpobj])
                    for gpobj in doids:
                        impure_gpchanges.setdefault(
                            gpobj, {
                                "new": set(),
                                "mod": set(),
                                "del": set()})["del"].update(doids[gpobj])
            else:
                self.type_to_objids[tpname] = set(impure_pccs[tp_obj])
                pcc_types_to_process.add(tpname)
        if impure_gpchanges:
            for tpobj, changes in impure_gpchanges.iteritems():
                new_oids, mod_oids, del_oids = (
                    changes["new"], changes["mod"], changes["del"])
                self.__process_get_pccs(
                    tpobj, new_oids, mod_oids, del_oids, final_record,
                    changelist, app)
        for tpname in pcc_types_to_process:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            new_oids, mod_oids, del_oids = self.__get_oid_change_buckets(
                tpname, changelist[tpname])

            self.__process_get_pccs(
                tp_obj, new_oids, mod_oids, del_oids, final_record,
                changelist, app)
        for tp_obj in impure_pccs:
            if tpname in self.type_to_objids:
                del self.type_to_objids[tpname]
        
        return {"gc": final_record}

    #################################################
    ### Private Methods #############################
    #################################################

    def __process_get_pccs(
            self, tp_obj, news, mods, dels, final_record,
            changelist, app):
        groupname = tp_obj.groupname
        tp_record = dict()
        new_oids = news.difference(
            set(final_record.setdefault(groupname, dict()).keys()))
        mod_oids = mods.difference(
            set(final_record.setdefault(groupname, dict()).keys()))
        del_oids = dels.difference(
            set(final_record.setdefault(groupname, dict()).keys()))
        if new_oids or mod_oids or del_oids:
            # The client is only pulling pcc record of this type. Not the
            # main type as well. We havent built updates, so pull updates.
            pdims = (
                tp_obj.projection_dims
                if hasattr(tp_obj, "projection_dims") else
                None)
            tp_record = (
                self.__get_dim_changes_for_basetype(
                    groupname, changelist[tp_obj.name],
                    new_oids, mod_oids, del_oids, app,
                    projection_dims=pdims))
            if not tp_record:
                final_record.setdefault(groupname, dict())

        # If there are new objs, deleted objects or some objects actually
        # changed, then set status of the types.
        if new_oids or del_oids or tp_record:
            final_record.setdefault(groupname, dict()).update(tp_record)
            self.__set_type_change_status(
                tp_obj.name, new_oids, mod_oids if tp_record else set(),
                del_oids, final_record[groupname])
            self.__set_latest_versions(
                groupname, new_oids, final_record[groupname])

    def __setup_join(self, joid, one_cross, tp_obj, final_record, changelist):
        join_record = {"dims": dict()}
        cross_oids = list()
        done = set()
        news, mods = dict(), dict()
        for npropname in one_cross:
            oid, _ = one_cross[npropname]
            cross_oids.append(oid)
            nproptp = tp_obj.namespaces[npropname].__rtypes_property_type__
            nptp_obj = nproptp.__rtypes_metadata__
            if (nptp_obj.name, oid) not in done:
                done.add((nptp_obj.name, oid))
                if (nptp_obj.name in changelist
                        and oid in changelist[nptp_obj.name]):
                    mods.setdefault(nptp_obj, set()).add(oid)
                else:
                    changelist.setdefault(nptp_obj.name, dict())
                    news.setdefault(nptp_obj, set()).add(oid)
            join_record["dims"][npropname] = {
                "type": Record.FOREIGN_KEY,
                "value": {
                    "group_key": nptp_obj.groupname,
                    "actual_type": {
                        "name": nptp_obj.name
                    },
                    "object_key": oid
                }
            }
        cross_oids = tuple(cross_oids)
        new_joid = (
            self.join_ids[tp_obj][cross_oids]
            if cross_oids in self.join_ids.setdefault(tp_obj, dict()) else
            joid)
        join_record["dims"][tp_obj.primarykey.name] = {
            "type": Record.STRING,
            "value": new_joid
        }
        self.join_ids[tp_obj][cross_oids] = new_joid
        is_mod = (
            tp_obj.name in changelist and new_joid in changelist[tp_obj.name])
        join_record["types"] = {
            tp_obj.name: Event.Modification if is_mod else Event.New}
        join_record["version"] = [
            changelist[tp_obj.name][new_joid] if is_mod else None,
            time.time()]
        final_record.setdefault(
            tp_obj.groupname, dict())[new_joid] = join_record
        return news, mods, dict()

    def __set_latest_versions(self, tpname, new_oids, final_changes):
        for oid in new_oids:
            final_changes[oid]["version"] = [
                None, self.type_to_obj_dimstate[tpname].lastkey(oid)]

    def __set_type_change_status(
            self, tpname, new_oids, mod_oids, del_oids, changes):
        for oid in new_oids:
            changes.setdefault(oid, dict()).setdefault(
                "types", dict())[tpname] = Event.New
        for oid in del_oids:
            changes.setdefault(oid, dict()).setdefault(
                "types", dict())[tpname] = Event.Delete
        for oid in mod_oids:
            # If there are no dim changes, it cannot be modification.
            if oid not in changes:
                continue
            changes[oid].setdefault(
                "types", dict())[tpname] = (
                    Event.Modification)

    def __get_oid_change_buckets(self, tpname, changelist):
        new_oids = self.type_to_objids[tpname].difference(
            set(changelist.keys()))
        mod_oids = self.type_to_objids[tpname].intersection(
            set(changelist.keys()))
        del_oids = set(changelist.keys()).difference(
            self.type_to_objids[tpname])
        return new_oids, mod_oids, del_oids


    def __get_dim_changes_for_basetype(
            self, tpname, changelist, new_oids,
            mod_oids, del_oids, app, projection_dims=None):
        group_changes = self.type_to_obj_dimstate[tpname]
        final_record = dict()
        for oid in new_oids:
            final_record[oid] = self.__merge_records(
                group_changes.get_full_obj(oid, app), projection_dims)
            final_record[oid]["version"] = [
                None,
                group_changes.lastkey(oid)]
        for oid in del_oids:
            final_record[oid] = dict()
        for oid in mod_oids:
            curr_vn = changelist[oid]
            dim_changes = self.__merge_records(
                group_changes.get_dim_changes_since(oid, curr_vn, app),
                projection_dims)
            latest_vn = group_changes.lastkey(oid)
            if latest_vn != curr_vn:
                final_record[oid] = dim_changes
                final_record[oid]["version"] = [curr_vn, latest_vn]

        return final_record

    def __merge_records(self, records, projection_dims):
        if not records:
            return dict()

        projection_dims_str = (
            set([d.name for d in projection_dims])
            if projection_dims else
            set())
        final_record = dict()
        final_record_dims = final_record.setdefault("dims", dict())

        for rec in records:
            if projection_dims:
                for dim, value in (
                        rec["dims"] if "dims" in rec else dict()).iteritems():
                    # In case of projection, copy only req dimensions.
                    if dim in projection_dims_str:
                        final_record_dims[dim] = value
            elif "dims" in rec:
                final_record_dims.update(rec["dims"])
        if final_record_dims:
            return final_record
        return dict()

    def __apply_changes(self, df_changes, except_app=None):
        '''
        all_changes is a dictionary in this format

        { <- gc stands for group changes
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
                    },
                    "version": [old_version, new_version]
                },
                <More object change logs> ...
            },
            <More group change logs> ...
        }
        '''
        tm = self.type_manager
        next_timestamp = time.time()
        objects_to_check_pcc = dict()
        deleted_objs = dict()
        for groupname, group_changes in df_changes.iteritems():
            if groupname not in self.type_to_obj_dimstate:
                continue
            group_changelist = self.type_to_obj_dimstate[groupname]

            for oid, obj_changes in group_changes.iteritems():
                prev_version, curr_version = obj_changes["version"]
                if not group_changelist.has_obj(oid):
                    if "dims" in obj_changes and prev_version is None:
                        # Should be a new object.
                        if oid not in self.type_to_objids[groupname]:
                            group_changelist.add_obj(
                                oid, curr_version, {"dims": obj_changes["dims"]},
                                except_app)
                            objects_to_check_pcc.setdefault(
                                groupname, set()).add(oid)
                            self.type_to_objids[groupname].add(oid)
                    elif "dims" in obj_changes:
                        raise RuntimeError(
                            "Something went wrong. Obj not in record, "
                            "but has last known version? What gives?")
                    # No dims no object, ignore and continue
                    continue
                if (groupname in obj_changes["types"]
                        and obj_changes["types"] == Event.Delete):
                    # Delete all records of the object. Not required any more.
                    group_changelist.delete_obj(oid)
                    deleted_objs.setdefault(groupname, set()).add(oid)
                    continue
                # Not a delete or a new object. (modification)
                objects_to_check_pcc.setdefault(
                    groupname, set()).add(oid)
                server_last_version = group_changelist.lastkey(oid)
                # latest version number might not be curr_version in case
                # of having to do a merge update.
                if server_last_version == prev_version:
                    # Alright, no need for transformations. Straightforward
                    # merge.
                    group_changelist.add_next_change(
                        oid, curr_version, {"dims": obj_changes["dims"]},
                        except_app)
                    # Do not need to change latest_version_number
                else:
                    # Have to do a merge.
                    changes_from_prev = self.__merge_records(
                        group_changelist.get_dim_changes_since(
                            oid, prev_version, except_app), None)
                    transformation = self.__calculate_transform(
                        changes_from_prev, {"dims": obj_changes["dims"]})
                    group_changelist.add_transformation(
                        oid, curr_version, {
                            "next_timestamp": next_timestamp,
                            "transform": transformation})
                    group_changelist.add_next_change(
                        oid, next_timestamp, {"dims": obj_changes["dims"]},
                        except_app)

        # start pcc calculations now.
        for groupname, oids in deleted_objs.iteritems():
            try:
                group_type = tm.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            for pcc_type in tm.meta_to_pure_members(group_type):
                if pcc_type.name in self.type_to_objids:
                    self.type_to_objids[pcc_type.name].difference_update(oids)

        for groupname, oids in objects_to_check_pcc.iteritems():
            try:
                group_type = tm.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            for oid in oids:
                dim_changes = df_changes[groupname][oid]["dims"]
                for pcc_type in tm.meta_to_pure_members(group_type):
                    if pcc_type.need_to_check(group_type, dim_changes):
                        if pcc_type.check_single_membership(
                                group_type, dim_changes,
                                self.type_to_obj_dimstate):
                            self.type_to_objids[pcc_type.name].add(oid)
                        elif oid in self.type_to_objids[pcc_type.name]:
                            self.type_to_objids[pcc_type.name].remove(oid)


    def __calculate_transform(self, inplace_changes, new_changes):
        new_changes = {"dims": dict()}
        if "dims" in inplace_changes:
            for dimname, dimchange in inplace_changes["dims"].iteritems():
                if dimname not in inplace_changes["dims"]:
                    new_changes["dims"][dimname] = dimchange
        return new_changes

    def __create_table(self, tpname, is_set):
        tp_obj = self.type_manager.get_requested_type_from_str(tpname)
        if is_set:
            self.type_to_obj_dimstate.setdefault(
                tp_obj.groupname,
                StateRecorder(tp_obj.groupname, self.maintain))
        self.type_to_objids.setdefault(tp_obj.groupname, set())
        self.type_to_objids.setdefault(tpname, set())
