'''
Created on Apr 19, 2016
@author: Rohan Achar
'''

import cbor
import hashlib
import importlib
import inspect
import os
from time import sleep
import sys, traceback
import re
from urlparse import urlparse, parse_qs

from rtypes.dataframe.dataframe_threading import dataframe_wrapper as dataframe_t
from rtypes.dataframe.dataframe import dataframe
from rtypes.dataframe.objectless_dataframe import ObjectlessDataframe as dataframe_ol
from rtypes.dataframe.application_queue import ApplicationQueue
from rtypes.pcc.utils.enums import Record

from spacetime.common.wire_formats import FORMATS
from spacetime.common.modes import Modes
from spacetime.common.converter import create_jsondict, create_complex_obj
from spacetime.common.crawler_generator import generate_datamodel

FETCHING_MODES = set([Modes.Getter, 
                      Modes.GetterSetter,
                      Modes.Taker])
TRACKING_MODES = set([Modes.Tracker])
PUSHING_MODES = set([Modes.Deleter,
                     Modes.GetterSetter,
                     Modes.Setter,
                     Modes.TakerSetter,
                     Modes.Producing])
ALL_MODES = set([Modes.Deleter,
                 Modes.GetterSetter,
                 Modes.Setter,
                 Modes.TakerSetter,
                 Modes.Producing,
                 Modes.Tracker,
                 Modes.Getter])

CRAWLER_SAVE_FILE = "spacetime_crawler_data"

class dataframe_stores(object):
    @property
    def is_alive(self):
        return self.master_dataframe.isAlive()

    def __init__(self, name2class, name2triggers, objectless_server):
        self.objectless_server = objectless_server
        self.master_dataframe = None
        self.app_to_df = dict()
        self.name2class = name2class
        self.name2triggers = name2triggers
        self.pause_servers = False
        self.app_wire_format = dict()
        self.app_wait_for_server = dict()
        self.instrument_filename = None
        self.app_to_stats = dict()
        self.check_base_dir_for_crawler_data()
        # self.master_dataframe.add_types(self.name2class.values())

    def __pause(self):
        while self.pause_servers:
            sleep(0.1)

    def start(self):
        self.master_dataframe = dataframe_t(
            dataframe=(
                dataframe()
                if not self.objectless_server else
                dataframe_ol()))
        self.master_dataframe.start()

    def add_new_dataframe(self, name, df):
        self.__pause()
        self.app_to_df[name] = df

    def delete_app(self, app):
        self.__pause()
        del self.app_to_df[app]

    def load_all_sets(self, app_name):
        # make all the stuff
        app_id = app_name.split("_")[-1]
        _, filename, _ = generate_datamodel(app_id)
        mod = importlib.import_module(
            "datamodel.search." + filename + "_datamodel")
        reload(mod)
        for name, cls in inspect.getmembers(mod):
            if hasattr(cls, "__rtypes_metadata__"):
                self.name2class[cls.__rtypes_metadata__.name] = cls
        return {
            Modes.Producing: set([
                self.name2class["datamodel.search.{0}_datamodel.{0}Link".format(
                    app_id)]]),
            Modes.GetterSetter: set([
                self.name2class["datamodel.search.{0}_datamodel."
                                "One{0}UnProcessedLink".format(app_id)]])
        }

    def parse_type(self, app, mode_map):
        # checks if the tpname is in dataframe yet
        if all(tpname in self.name2class
               for mode, mode_types in mode_map.iteritems()
               for tpname in mode_types):
            return {mode: [self.name2class[tpname] for tpname in mode_types]
                    for mode, mode_types in mode_map.iteritems()}
        else:
            # builds the appropriate types and files
            return self.load_all_sets(app)

    def register_app(self, app, type_map,
                     wire_format="json", wait_for_server=False):
        self.__pause()
        # TODO: Make a way to add types here
        # used this throughout the function instead of "type_map"
        real_type_map = self.parse_type(app, type_map)

        # Add all types to master.
        types_to_add_to_master = set()
        for mode in ALL_MODES:
            types_to_add_to_master.update(
                set(real_type_map.setdefault(mode, set())))


        all_types = [
            self.name2class[tpam.__rtypes_metadata__.name]
            for tpam in types_to_add_to_master]
        self.master_dataframe.add_types(all_types)

        # Look at invidual types.
        if not self.objectless_server:
            types_to_get = set()
            for mode in FETCHING_MODES:
                types_to_get.update(set(type_map.setdefault(mode, set())))
            types_to_track = set()
            for mode in TRACKING_MODES:
                types_to_track.update(set(type_map.setdefault(mode, set())))
            types_to_track = types_to_track.difference(types_to_get)
            real_types_to_get = [self.name2class[tpg] for tpg in types_to_get]
            real_types_to_track = [
                self.name2class[tpt] for tpt in types_to_track]

            df = ApplicationQueue(
                app, real_types_to_get + real_types_to_track,
                self.master_dataframe)
            self.add_new_dataframe(app, df)
        else:
            # Just required for the server to not disconnect apps not registered
            self.app_to_df[app] = None
            try:
                state_manager = self.master_dataframe.dataframe.state_manager
                undownloaded = (
                    len(state_manager.type_to_objids[
                        "datamodel.search.{0}_datamodel."
                        "{0}UnprocessedLink".format(app)]))
                total = (len(state_manager.type_to_objids[
                        "datamodel.search.{0}_datamodel.{0}Link".format(app)]))
                self.app_to_stats[app] = (total - undownloaded, undownloaded)
            except KeyError:
                self.app_to_stats[app] = (0, 0)
        # Adding to name2class
        for tp in all_types:
            self.name2class.setdefault(tp.__rtypes_metadata__.name, tp)

        # setting the wire format for the app.
        self.app_wire_format[app] = wire_format

        # Setting interaction mode.
        self.app_wait_for_server[app] = wait_for_server

    def disconnect(self, app):
        self.__pause()
        if app in self.app_to_df:
            self.delete_app(app)

    def reload_dms(self, datamodel_types):
        self.__pause()
        pass

    def mark_as_downloaded(self, link_key, obj_changes):
        link_as_file = self.make_link_into_file(link_key)
        if not os.path.exists(link_as_file):
            # add the data to the file
            link_data = {
                dimname: dimchange
                for dimname, dimchange in (
                    obj_changes["dims"].iteritems())
                if dimname is not "download_complete"}
            
            cbor.dump(link_data, open(link_as_file, "wb"))

        new_data = {
            "download_complete": {
                "type": Record.BOOL, "value": True}}
        if "error_reason" in obj_changes["dims"]:
            new_data["error_reason"] = (
                obj_changes["dims"]["error_reason"])
        else:
            new_data["error_reason"] = {
                "type": Record.STRING, "value": ""}
        obj_changes["dims"] = new_data

    def check_uploaded(self, app, link_key):
        url = "http://" + link_key
        if not self.is_valid(url):
            if not os.path.exists(INVALIDS):
                os.makedirs(INVALIDS)
                invalid_f = os.join(INVALIDS, app)
                open(invalid_f, "a").write(
                    "{0} :: {1}\n".format(time.time(), url))

    # spacetime automatically pushing changes into server
    def update(self, app, changes, callback=None):
        try:
            self.__pause()
            dfc_type, _ = FORMATS[self.app_wire_format[app]]
            dfc = dfc_type()
            dfc.ParseFromString(changes)
            # print "DFC :::: ", dfc
            downloaded, undownloaded = self.app_to_stats[app]
            if app.startswith("CrawlerFrame_"):
                crawler_user = app[len("CrawlerFrame_"):]
                group_tpname = "datamodel.search.{0}_datamodel.{0}Link".format(
                    crawler_user)
                if group_tpname in dfc["gc"]:
                    for link_key, obj_changes in (
                            dfc['gc'][group_tpname].iteritems()):
                        if ("download_complete" in obj_changes["dims"] 
                                and obj_changes["dims"][
                                    "download_complete"]["value"]):
                            self.mark_as_downloaded(link_key, obj_changes)
                            downloaded += 1
                            undownloaded -= 1
                        else:
                            self.check_uploaded(crawler_user, link_key)
                            undownloaded += 1

                    self.app_to_stats[app] = (downloaded, undownloaded)
            if app in self.app_to_df:
                self.master_dataframe.apply_changes(
                    dfc, except_app=app,
                    wait_for_server=self.app_wait_for_server[app])
            # before this
            if callback:
                callback(app)
        except Exception, e:
            print "U ERROR!!!", e, e.__class__.__name__
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            raise

    # thier pull into client
    def getupdates(self, app, changelist=None, callback=None):
        """ The client is pulling info from the server """
        try:
            self.__pause()
            dfc_type, content_type = FORMATS[self.app_wire_format[app]]
            final_updates = dfc_type()
            if self.objectless_server:
                # change this before callback
                final_updates = dfc_type(
                    self.master_dataframe.get_record(changelist, app))
                if app.startswith("CrawlerFrame_"):
                    crawler_name = app[len("CrawlerFrame_"):]
                    group_tpname = (
                        "datamodel.search.{0}_datamodel.{0}Link".format(
                            crawler_name))
                    if group_tpname in final_updates["gc"]:
                        for link_key, link_changes in (
                                final_updates['gc'][group_tpname].iteritems()):
                            if "dims" in link_changes:
                                link_as_file = self.make_link_into_file(
                                    link_key)
                                if os.path.exists(link_as_file):
                                    # this means that the data
                                    # already exist on disk
                                    # so grab the data rather
                                    # than downloading it
                                    data = cbor.load(open(link_as_file, "rb"))
                                    link_changes["dims"].update(data)
                    final_updates["stats"] = self.app_to_stats[app]
            else:
                if app in self.app_to_df:
                    final_updates = dfc_type(self.app_to_df[app].get_record())
                    self.app_to_df[app].clear_record()
            if callback:
                callback(app, final_updates.SerializeToString(), content_type)
            else:
                return final_updates.SerializeToString(), content_type
        except Exception, e:
            print "GU ERROR!!!", e, e.__class__.__name__
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            raise

    def check_base_dir_for_crawler_data(self):
        """ Make sure that the base dir used to stor the crawler data exist """
        if not os.path.exists(CRAWLER_SAVE_FILE):
            os.makedirs(CRAWLER_SAVE_FILE)

    def make_link_into_file(self, link):
        try:
            hashed_link = hashlib.sha224(link).hexdigest()
        except UnicodeEncodeError:
            try:
                hashed_link = hashlib.sha224(link.encode("utf-8")).hexdigest()
            except UnicodeEncodeError:
                hashed_link = str(hash(link))
        
        return os.path.join(CRAWLER_SAVE_FILE, hashed_link)

    def get_app_list(self):
        return self.app_to_df.keys()

    def clear(self, tp=None):
        if not tp:
            self.shutdown()
            print "Restarting master dataframe."
            self.__init__(
                self.name2class, self.name2triggers, self.objectless_server)
            self.start()
        else:
            if tp in self.master_dataframe.object_map:
                del self.master_dataframe.object_map[tp]
            if tp in self.master_dataframe.current_state:
                del self.master_dataframe.current_state[tp]

    def shutdown(self):
        print "Shutting down master dataframe."
        if self.master_dataframe:
            self.master_dataframe.shutdown()
            self.master_dataframe.join()
        print "Master dataframe has shut down."

    def pause(self):
        self.pause_servers = True

    def unpause(self):
        self.pause_servers = False

    def gc(self, sim):
        # For now not clearing contents
        self.delete_app(sim)

    def get(self, tp):
        return [create_jsondict(o) for o in self.master_dataframe.get(tp)]

    def put(self, tp, objs):
        real_objs = [
            create_complex_obj(tp, obj, self.master_dataframe.object_map)
            for obj in objs.values()]
        tpname = tp.__rtypes_metadata__.name
        gkey = self.master_dataframe.member_to_group[tpname]
        if gkey == tpname:
            self.master_dataframe.extend(tp, real_objs)
        else:
            for obj in real_objs:
                oid = obj.__primarykey__
                if oid in self.master_dataframe.object_map[gkey]:
                    # do this only if the object is already there.
                    # cannot add an object if it is a subset
                    # (or any other pcc type) type if it doesnt exist.
                    for dim in obj.__dimensions__:
                        # setting attribute to the original object,
                        # so that changes cascade
                        setattr(
                            self.master_dataframe.object_map[gkey][oid],
                            dim._name, getattr(obj, dim._name))

    def save_instrumentation_data(self):
        pass

    def is_valid(self, url):
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return (".ics.uci.edu" in parsed.hostname
                and not re.match(
                    ".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4"
                    "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                    "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe"
                    "|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1"
                    "|thmx|mso|arff|rtf|jar|csv"
                    "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
                and "calendar.ics.uci.edu" not in parsed.hostname
                and "archive.ics.uci.edu/ml/dataset?" not in url
                and "ganglia.ics.uci.edu" not in url)
        except TypeError:
            print ("TypeError for ", parsed)
            return False