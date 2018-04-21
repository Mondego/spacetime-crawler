'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import

import logging
import time
import os
from threading import Thread as Parallel

from spacetime.common.modes import Modes
from rtypes.dataframe.dataframe_changes.IDataframeChanges import DataframeChanges_Base
from rtypes.dataframe.dataframe_client import dataframe_client as dataframe_cl
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary
from .IFrame import IFrame


class ClientFrame(IFrame):  # pylint: disable=R0902
    '''Class that is the interface between the server connection and app.'''

    @property
    def appname(self):
        return self._appname

    def __init__(
            self, connector, dataframe=dataframe_cl(),
            time_step=500, logfile=None):
        self.connector = connector
        self.thread = None
        self.object_store = dataframe
        self.object_store.start_recording = True
        self.is_instrumented = False

        self._app = None
        self._appname = ""
        self._host_typemap = dict()
        self._typemap = dict()
        self._time_step = (float(time_step) / 1000)
        self._observed_types = set()
        self._observed_types_new = set()
        self._observed_types_mod = set()
        self._producing_types = set()
        self._deleting_types = set()
        self._curtime = time.time()
        self._curstep = 0
        self._start_time = time.strftime("%Y-%m-%d_%H-%M-%S")
        self._sessions = dict()
        self._host_to_push_groupkey = dict()
        self._host_to_connector = dict()
        self._disconnected = False
        self.logger = ClientFrame.setup_logger(
            "spacetime@" + self._appname, file_path=logfile)
        super(ClientFrame, self).__init__(
            connector, dataframe, time_step, logfile)

#####################################
# Console hooks
#####################################

    def get_instrumented(self):
        """
        Returns if frame is running instrumentation. (True/False)
        """
        return self.is_instrumented

    def get_curtime(self):
        """
        Returns the timestamp of the current step.
        """
        return self._curtime

    def get_curstep(self):
        """
        Returns the current step value of the simulation.
        """
        return self._curstep

    def get_timestep(self):
        """
        Returns the time-step value in milliseconds.
        """
        return self._time_step

    def app_done(self):
        """
        app_done

        Returns whether app has finished running or not
        """
        return self._app.done

#####################################
# Set up APIs
#####################################

    def attach_app(self, app, appname=None):
        """
        Receives reference to application (implementing IApplication).

        Arguments:
        app : spacetime-conformant Application

        Exceptions:
        None
        """
        self._app = app
        self._appname = (
            app.__class__.__name__  + "_" + self._app.app_id
            if not appname else
            appname)

    def run_async(self):
        """
        Starts application in non-blocking mode.

        Arguments:
        None

        Exceptions:
        None
        """
        self.thread = Parallel(
            target=self._run, name="Thread_frame_{0}".format(self._appname))
        self.thread.daemon = True
        self.thread.start()

    def run_main(self):
        """
        Run the application in the main thread (Blocking mode).

        Arguments:
        None

        Exceptions:
        None
        """
        self._run()

    def run(self):
        """
        Run the application in the main thread (Blocking mode).

        Arguments:
        None

        Exceptions:
        None
        """
        self._run()

    def shutdown(self):
        self._stop()

#####################################
# Functions for Application to use.
#####################################

    def get(self, tp, oid=None):
        """
        Retrieves objects from local data storage. If id is provided, returns
        the object identified by id. Otherwise, returns the list of all objects
        matching type tp.

        Arguments:
        tp : PCC set type being fetched
        oid : primary key of an individual object.

        Exceptions:
        - ID does not exist in store
        - Application does not annotate that type
        """
        if tp in self._observed_types:
            if oid:
                # Have to get this to work
                return self.object_store.get(tp, oid)
            return self.object_store.get(tp)
        else:
            raise Exception(
                "Application %s does not annotate type %s" % (
                    self._appname, tp))

    def add(self, obj):
        """
        Adds an object to be stored and tracked by spacetime.

        Arguments:
        obj : PCC object to stored

        Exceptions:
        - Application is not annotated as a producer
        """
        if obj.__class__ in self._producing_types:
            self.object_store.append(obj.__class__, obj)
        else:
            raise Exception(
                "Application %s is not a producer of type %s" % (
                    self._appname, obj.__class__))

    def delete(self, tp, obj):
        """
        Deletes an object currently stored and tracked by spacetime.

        Arguments:
        tp: PCC type of object to be deleted
        obj : PCC object to be deleted

        Exceptions:
        - Application is not annotated as a Deleter
        """

        if tp in self._deleting_types:
            self.object_store.delete(tp, obj)
        else:
            raise Exception(
                "Application %s is not registered to delete %s" % (
                    self._appname, tp))

    def get_new(self, tp):
        """
        Retrieves new objects of type "tp" retrieved in last pull (i.e. since
        last tick).

        Arguments:
        tp: PCC type for retrieving list of new objects

        Exceptions:
        None

        Note:
        Application should be annotated as  a Getter, GetterSetter, or Tracker,
        otherwise result is always an empty list.
        """
        if tp in self._observed_types_new:
            return self.object_store.get_new(tp)
        self.logger.warn(
            "Checking for new objects of type %s, but not "
            "a Getter, GetterSetter, or Tracker of type. Empty list "
            "always returned", tp)
        return list()

    def get_mod(self, tp):
        """
        Retrieves objects of type "tp" that were modified since last pull
        (i.e. since last tick).

        Arguments:
        tp: PCC type for retrieving list of modified objects

        Exceptions:
        None

        Note:
        Application should be annotated as a Getter,or GetterSetter, otherwise
        result is always an empty list.
        """
        if tp in self._observed_types_mod:
            return self.object_store.get_mod(tp)
        self.logger.warn(
            "Checking for modifications in objects of type "
            "%s, but not a Getter or GetterSetter of type. "
            "Empty list always returned", tp)
        return list()

    def get_deleted(self, tp):
        """
        Retrieves objects of type "tp" that were deleted since last pull
        (i.e. since last tick).

        Arguments:
        tp: PCC type for retrieving list of deleted objects

        Exceptions:
        None

        Note:
        Application should be annotated as a Getter, GetterSetter, or Tracker,
        otherwise result is always an empty list.
        """
        if tp in self._observed_types_new:
            return self.object_store.get_deleted(tp)
        self.logger.warn(
            "Checking for deleted objects of type %s, but "
            "not a Getter, GetterSetter, or Tracker of type. Empty list "
            "always returned", tp)
        return list()

#####################################
# Internal functions for driving client.
#####################################

    def _register_app(self):
        self._host_typemap = {}
        for address, tpmap in self._app.__declaration_map__.items():
            for declaration in tpmap:
                self._host_typemap.setdefault(address, dict()).setdefault(
                    declaration, set()).update(set(tpmap[declaration]))

        all_types = set()

        for host in self._host_typemap:
            self.connector.add_host(host, self._host_typemap[host])

            (producing, getting, gettingsetting,
             deleting, setting, tracking) = (
                 self._host_typemap[host].setdefault(Modes.Producing, set()),
                 self._host_typemap[host].setdefault(Modes.Getter, set()),
                 self._host_typemap[host].setdefault(
                     Modes.GetterSetter, set()),
                 self._host_typemap[host].setdefault(Modes.Deleter, set()),
                 self._host_typemap[host].setdefault(Modes.Setter, set()),
                 self._host_typemap[host].setdefault(Modes.Tracker, set()))

            all_types_host = tracking.union(
                producing).union(getting).union(gettingsetting).union(
                    deleting).union(setting)
            all_types.update(all_types_host)
            self._producing_types.update(producing)
            self._deleting_types.update(deleting)
            self._observed_types.update(all_types_host)
            self._observed_types_new.update(
                tracking.union(getting).union(gettingsetting))
            self._observed_types_mod.update(getting.union(gettingsetting))

        self.object_store.add_types(all_types)

        for host in self._host_typemap:
            self._host_to_push_groupkey[host] = set(
                [self.object_store.get_group_key(tp)
                 for tp in self._host_typemap[host][Modes.GetterSetter].union(
                     self._host_typemap[host][Modes.Setter]).union(
                         self._host_typemap[host][Modes.Producing]).union(
                             self._host_typemap[host][Modes.Deleter])])

        for host in self._host_typemap:
            self.connector.register(host)
        return True

    def _run(self):
        self._clear()
        if not self._app:
            raise NotImplementedError("App has not been attached")
        if self._register_app():
            try:
                self._pull()
                self._app.initialize()
                self._push()
                while not self._app.done:
                    st_time = time.time()
                    self._one_step()
                    end_time = time.time()
                    timespent = end_time - st_time
                    self._curstep += 1
                    self._curtime = time.time()
                    # time spent on execution loop
                    if timespent < self._time_step:
                        time.sleep(float(self._time_step - timespent))
                    else:
                        self.logger.info(
                            "loop exceeded maximum time: %s ms", timespent)

                # One last time, because _shutdown may
                # delete objects from the store
                self._pull()
                self._shutdown()
                self._push()
                self._unregister_app()
            except:
                self.logger.exception("An unknown error occurred.")
                raise
        else:
            self.logger.info("Could not register, exiting run loop...")

    def _one_step(self):
        self._pull()
        self._update()
        self._push()

    def _pull(self):
        if self._disconnected:
            return
        updates = DataframeChanges_Base()
        is_only_diff = None
        for host in self._host_typemap:
            # Need to give mechanism to selectively ask for some changes.
            # Very hard to implement in current dataframe scheme.
            success, only_diff, update = self.connector.get_updates(host)
            if is_only_diff is None:
                is_only_diff = only_diff
            else:
                is_only_diff = is_only_diff and only_diff
            updates.CopyFrom(update)
        self._process_pull_resp(is_only_diff, updates)
        if not success:
            self.logger.exception("Disconnected from host.")
            self._disconnected = True
            self._stop()

    def _process_pull_resp(self, only_diff, resp):
        if resp and "gc" in resp:
            if self.connector.delete_joins:
                self.object_store.clear_joins()
            self.object_store.apply_changes(
                resp, track=False, only_diff=only_diff)

    def _update(self):
        self._app.update()

    def _push(self):
        if self._disconnected:
            return
        changes = self.object_store.get_record()

        for host in self._host_typemap:
            changes_for_host = dict()
            changes_for_host["gc"] = RecursiveDictionary({
                gck: gc
                for gck, gc in changes["gc"].iteritems()
                if gck in self._host_to_push_groupkey[host]})
            if "types" in changes:
                changes_for_host["types"] = changes["types"]
            success = self.connector.update(host, changes_for_host)
            if not success:
                self.logger.exception("Disconnected from host.")
                self._disconnected = True
                self._stop()

        self.object_store.clear_record()
        self.object_store.clear_buffer()

    def _shutdown(self):
        """
        _shutdown

        Called after the frame execution loop stops, in the last pull/push
        iteration
        """
        self._app.shutdown()

    def _clear(self):
        self._disconnected = False
        self._app.done = False
        self.object_store.clear_all()

    def _stop(self):
        """
        _stop

        Called by frame's command prompt on quit/exit
        """
        self._app.done = True

    def _unregister_app(self):
        for host in self._host_typemap:
            self.connector.disconnect(host)
            self.logger.info("Successfully deregistered from %s", host)

#####################################
# Static methods.
#####################################

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
        # logging.getLogger("requests").setLevel(logging.WARNING)
