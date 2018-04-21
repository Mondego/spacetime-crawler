from __future__ import absolute_import
import logging
import os
from spacetime.common.modes import Modes
from rtypes.connectors.sql import RTypesMySQLConnection
from rtypes.pcc.utils.enums import Event

class MySqlConnection(object):
    @staticmethod
    def setup_logger(name, file_path=None):
        """
        Set up the loggers for this client frame.

        Arguments:
          name: Name of the client application.
          file_path: logfile to write logs into.

        Exceptions:
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

    def __init__(self, app_id, address="127.0.0.1", user=None, password=None, database=None):
        self.user = user
        self.password = password
        self.database = database
        self.default_address = address
        self.host_to_connection = dict()
        self.host_to_typemap = dict()
        self.host_to_pccmap = dict()
        self.host_to_read_types = dict()
        self.host_to_all_types = dict()
        self.logger = MySqlConnection.setup_logger(
            "spacetime-connector" + app_id)
        self.delete_joins = True

    def add_host(self, host, typemap):
        if host == "default":
            host = self.default_address
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

    def register(self, host):
        if host == "default":
            host = self.default_address
        self.host_to_connection[host] = RTypesMySQLConnection(
            user=self.user, password=self.password,
            host=host, database=self.database)
        connection = self.host_to_connection[host]
        all_types = self.host_to_all_types[host]
        pcc_map = self.host_to_pccmap[host]
        connection.__rtypes_write__({
            "types": {
                tp.__rtypes_metadata__.name: Event.New
                for tp in all_types}}, pcc_map)
        return True

    def update(self, host, changes):
        if host == "default":
            host = self.default_address
        connection = self.host_to_connection[host]
        pcc_map = self.host_to_pccmap[host]
        connection.__rtypes_write__(changes, pcc_map)
        return True

    def get_updates(self, host):
        if host == "default":
            host = self.default_address
        read_types = self.host_to_read_types[host]
        connection = self.host_to_connection[host]
        results, not_diff = connection.__rtypes_query__(read_types)
        return True, not not_diff, results

    def disconnect(self, host):
        if host == "default":
            host = self.default_address
        connection = self.host_to_connection[host]
        all_types = self.host_to_all_types[host]
        pcc_map = self.host_to_pccmap[host]
        connection.__rtypes_write__({
            "types": {
                tp.__rtypes_metadata__.name: Event.Delete
                for tp in all_types}}, pcc_map)
