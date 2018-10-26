from __future__ import absolute_import
import json
import logging
import platform
import time
import os

import requests
from requests.sessions import Session
from requests.exceptions import HTTPError, ConnectionError
import cbor

from rtypes.pcc.utils.enums import Event
from rtypes.pcc.triggers import TriggerProcedure
from spacetime.common.javahttpadapter import MyJavaHTTPAdapter, ignoreJavaSSL
from spacetime.common.wire_formats import FORMATS
from spacetime.common.modes import Modes

def setup_logger(name, file_path=None):
    """
    Set up the loggers for this client frame.

    Arguments:
        name: Name of the client application.
        file_path: logfile to write logs into.

    Exce Clearptions:
    None
    """

    logger = logging.getLogger(name)
    # Set default logging handler to avoid "No handler found" warnings.
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.DEBUG)
    if file_path:
        folder = os.path.dirname(file_path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        flog = logging.handlers.RotatingFileHandler(
            file_path, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
        flog.setLevel(logging.DEBUG)
        flog.setFormatter(
            logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
        logger.addHandler(flog)

    logger.debug("Starting logger for %s", name)
    return logger


class SpacetimeConnection(object):
    def __init__(
            self, app_id, address="http://127.0.0.1:12000/", wire_format="cbor",
            wait_for_server=False, logfile=None):
        self.host_to_connection = dict()
        self.host_to_typemap = dict()
        self.host_to_pccmap = dict()
        self.host_to_read_types = dict()
        self.host_to_all_types = dict()
        self.wait_for_server = wait_for_server
        self.wire_format = wire_format
        self.app_id = app_id
        self.default_address = address.rstrip("/") + "/"
        self.logger = setup_logger("spacetime-connector" + app_id, logfile)
        self.delete_joins = True

    def __handle_request_errors(self, resp):
        if resp.status_code == 401:
            self.logger.error(
                "This application is not registered at the server. Stopping..")
            raise RuntimeError(
                "This application is not registered at the server.")
        else:
            self.logger.warn(
                "Non-success code received from server: %s %s",
                resp.status_code, resp.reason)

    def add_host(self, hostpart, typemap):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        self.host_to_connection[host] = None
        self.host_to_typemap[host] = typemap
        all_types = list()
        for mode in typemap:
            if mode != Modes.Triggers:
                all_types.extend(typemap[mode])
        read_types = list()
        for mode in typemap:
            if mode not in set([Modes.Triggers,
                                Modes.Deleter,
                                Modes.Producing]):
                read_types.extend(typemap[mode])
        pcc_map = {
            tp.__rtypes_metadata__.name: tp
            for tp in all_types}

        self.host_to_pccmap[host] = pcc_map
        self.host_to_read_types[host] = read_types
        self.host_to_all_types[host] = all_types

    def register(self, hostpart):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        jobj = {
            k: [tp.name
                if isinstance(tp, TriggerProcedure) else
                tp.__rtypes_metadata__.name for tp in v]
            for k, v in self.host_to_typemap[host].iteritems()}
        jsonobj = json.dumps(
            {"sim_typemap": jobj, "wire_format": self.wire_format,
             "app_id": self.app_id, "wait_for_server": self.wait_for_server})
        try:
            self.host_to_connection[host] = Session()
            if platform.system() == "Java":
                ignoreJavaSSL()
                self.logger.info("Using custom HTTPAdapter for Jython")
                self.host_to_connection[host].mount(
                    host, MyJavaHTTPAdapter())
                self.host_to_connection[host].verify = False
            self.logger.info("Registering with %s", host)
            resp = requests.put(
                host,
                data=jsonobj,
                headers={"content-type": "application/octet-stream"})
        except HTTPError:
            self.__handle_request_errors(resp)
            return False
        except ConnectionError:
            self.logger.exception("Cannot connect to host.")
            return False

    def update(self, hostpart, changes_dict):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        try:
            df_cls, content_type = FORMATS[self.wire_format]
            changes = df_cls()
            changes.CopyFrom(changes_dict)
            dictmsg = changes.SerializeToString()
            headers = {"content-type": content_type}
            resp = self.host_to_connection[host].post(
                host + "/postupdated", data=dictmsg, headers=headers)
        except TypeError:
            self.logger.exception(
                "error encoding obj. Object: %s", changes)
        except HTTPError:
            self.__handle_request_errors(resp)
        except ConnectionError:
            return False
        return True

    def get_updates(self, hostpart):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        resp = self.host_to_connection[host].get(
            host + "/getupdated", data=dict())
        df_cls, _ = (
            FORMATS[self.wire_format])
        dataframe_change = df_cls()
        try:
            resp.raise_for_status()
            data = resp.content
            dataframe_change.ParseFromString(data)
            return True, True, dataframe_change
        except HTTPError:
            self.__handle_request_errors(resp)
        except ConnectionError:
            pass
        return False, True, dataframe_change

    def disconnect(self, hostpart):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        _ = requests.delete(host)


class ObjectlessSpacetimeConnection(object):
    def __init__(
            self, app_id, address="http://127.0.0.1:12000/", wire_format="cbor",
            wait_for_server=False, debug=False, logfile=None):
        self.host_to_connection = dict()
        self.host_to_typemap = dict()
        self.host_to_pccmap = dict()
        self.host_to_read_types = dict()
        self.host_to_all_types = dict()
        self.host_to_changelist = dict()
        self.wait_for_server = wait_for_server
        self.debug = debug
        self.app_id = app_id
        self.wire_format = wire_format
        self.default_address = address.rstrip("/") + "/"
        self.logger = setup_logger(
            "spacetime-connector" + app_id, logfile)
        self.delete_joins = False

    def __handle_request_errors(self, resp):
        if resp.status_code == 401:
            self.logger.error(
                "This application is not registered at the server. Stopping..")
            raise RuntimeError(
                "This application is not registered at the server.")
        else:
            self.logger.warn(
                "Non-success code received from server: %s %s",
                resp.status_code, resp.reason)

    def add_host(self, hostpart, typemap):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        self.host_to_connection[host] = None
        self.host_to_typemap[host] = typemap
        all_types = list()
        for mode in typemap:
            if mode != Modes.Triggers:
                all_types.extend(typemap[mode])
        read_types = list()
        for mode in typemap:
            if mode not in set([Modes.Triggers,
                                Modes.Deleter,
                                Modes.Producing,
                                Modes.Setter]):
                read_types.extend(typemap[mode])
        pcc_map = {
            tp.__rtypes_metadata__.name: tp
            for tp in all_types}

        self.host_to_pccmap[host] = pcc_map
        self.host_to_read_types[host] = read_types
        self.host_to_all_types[host] = all_types
        self.host_to_changelist[host] = {
            tpname: dict()
            for tpname in pcc_map if pcc_map[tpname] in read_types}

    def register(self, hostpart):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        jobj = {
            k: [tp.name
                if isinstance(tp, TriggerProcedure) else
                tp.__rtypes_metadata__.name for tp in v]
            for k, v in self.host_to_typemap[host].iteritems()}
        jsonobj = json.dumps(
            {"sim_typemap": jobj, "wire_format": self.wire_format,
             "app_id": self.app_id, "wait_for_server": self.wait_for_server})
        try:
            self.host_to_connection[host] = Session()
            if platform.system() == "Java":
                ignoreJavaSSL()
                self.logger.info("Using custom HTTPAdapter for Jython")
                self.host_to_connection[host].mount(
                    host, MyJavaHTTPAdapter())
                self.host_to_connection[host].verify = False
            self.logger.info("Registering with %s", host)
            resp = requests.put(
                host,
                data=jsonobj,
                headers={"content-type": "application/octet-stream"})
        except HTTPError:
            self.__handle_request_errors(resp)
            return False
        except ConnectionError:
            self.logger.exception("Cannot connect to host.")
            return False

    def add_versions(self, host, changes):
        new_changelist = time.time()
        current_version = self.host_to_changelist[host]
        if "gc" not in changes:
            return
        for grp_name, grp_changes in changes["gc"].iteritems():
            current_grp_version = current_version.setdefault(grp_name, dict())
            for oid, obj_changes in grp_changes.iteritems():
                current_dim_version = current_grp_version.setdefault(
                    oid, None)

                # New dimension version is set if the dimensions have changed.
                new_dim_vesion = (
                    new_changelist
                    if "dims" in obj_changes and obj_changes["dims"] else
                    current_dim_version)

                # Set the change [from, to] for sending to server.
                changelist = [current_dim_version, new_dim_vesion]
                # Update local records to show current dim version.
                obj_changes["version"] = changelist

                # Calculating type change versions.
                for tpname, status in obj_changes["types"].iteritems():
                    if status != Event.Delete:
                        current_version.setdefault(
                            tpname, dict())[oid] = new_dim_vesion
                    else:
                        if oid in current_version.setdefault(tpname, dict()):
                            del current_version[tpname][oid]

    def get_versions(self, host):
        pcc_map = self.host_to_pccmap[host]
        read_types = self.host_to_read_types[host]
        return {
            tpname: states
            for tpname, states in self.host_to_changelist[host].iteritems()
            if tpname in pcc_map and pcc_map[tpname] in read_types}

    def set_incoming_versions(self, host, changes):
        current_version = self.host_to_changelist[host]
        if "gc" not in changes:
            return
        for grp_name, grp_changes in changes["gc"].iteritems():
            for oid, obj_changes in grp_changes.iteritems():
                if "version" in obj_changes:
                    _, new_version = obj_changes["version"]
                    current_version.setdefault(
                        grp_name, dict())[oid] = new_version
                else:
                    new_version = None

                for tpname, status in obj_changes["types"].iteritems():
                    if status != Event.Delete:
                        # This should never actually be None.
                        current_version.setdefault(
                            tpname, dict())[oid] = new_version
                    else:
                        if oid in current_version.setdefault(tpname, dict()):
                            del current_version[tpname][oid]

    def update(self, hostpart, changes_dict):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        try:
            df_cls, content_type = FORMATS[self.wire_format]
            self.add_versions(host, changes_dict)
            if self.debug:
                json.dump(
                    changes_dict,
                    open("push_{0}.json".format(self.app_id), "a"),
                    sort_keys=True, separators=(",", ": "), indent=4)
            changes = df_cls()
            changes.CopyFrom(changes_dict)
            dictmsg = changes.SerializeToString()
            headers = {"content-type": content_type}
            resp = self.host_to_connection[host].post(
                host + "/postupdated", data=dictmsg, headers=headers)
        except TypeError:
            self.logger.exception(
                "error encoding obj. Object: %s", changes)
        except HTTPError:
            self.__handle_request_errors(resp)
        except ConnectionError:
            return False
        return True

    def get_request(self, host, data, headers):
        return self.host_to_connection[host].get(
            host + "/getupdated", data=data, headers=headers)

    def get_updates(self, hostpart):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        headers = {"content-type": "application/octet-stream"}
        data_d = self.get_versions(host)
        if self.debug:
            json.dump(
                data_d,
                open("pre_pull_{0}.json".format(self.app_id), "a"),
                sort_keys=True, separators=(",", ": "), indent=4)
        data = cbor.dumps(data_d)
        resp = self.get_request(host, data, headers)
        df_cls, _ = (
            FORMATS[self.wire_format])
        dataframe_change = df_cls()
        try:
            resp.raise_for_status()
            data = resp.content
            dataframe_change.ParseFromString(data)
            if self.debug:
                json.dump(
                    dataframe_change,
                    open("post_pull_{0}.json".format(self.app_id), "a"),
                    sort_keys=True, separators=(",", ": "), indent=4)
            self.set_incoming_versions(host, dataframe_change)
            self.get_crawler_stats(dataframe_change)
            return True, True, dataframe_change
        except HTTPError:
            self.__handle_request_errors(resp)
        except ConnectionError:
            pass
        return False, True, dataframe_change

    def disconnect(self, hostpart):
        if hostpart == "default":
            hostpart = self.default_address
        host = hostpart + self.app_id
        _ = requests.delete(host)

    def get_crawler_stats(self, dataframe_change):
        if "stats" in dataframe_change:
            print "PULL complete. Number of links in server {0} (D) + {1} (UD)".format(*(dataframe_change["stats"]))
