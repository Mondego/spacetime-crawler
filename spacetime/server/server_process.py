import json
import logging
import os
import time
import sys
from functools import wraps
from threading import Thread
from threading import Timer

from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import Event
from multiprocessing.pool import ThreadPool

import cbor
from tornado.web import RequestHandler, HTTPError, asynchronous
import tornado.ioloop

from spacetime.server.server_requests import RestartStoreRequest, SetUpRequest, ShutdownRequest, StartRequest, GetQueueSizeRequest
from spacetime.server.console import SpacetimeConsole

class BaseRegisterHandler(RequestHandler):
    pass


class BaseAllUpdatedTracked(RequestHandler):
    pass

def get_exception_handler(timers, store, logger):
    def handle_exceptions(f):
        @wraps(f)
        def wrapped(*args, **kwds):
            try:
                timers[args[1]] = time.time()
                if not isinstance(args[0], BaseRegisterHandler):
                    if args[1] not in store.get_app_list():
                        raise HTTPError(
                            500, "%s not registered to the store." % args[1])
                ret = f(*args, **kwds)
            except Exception as e:
                print "BEEP", e, e.__class__.__name__
                logger.exception(
                    "Exception %s handling function %s:", repr(e), f.func_name)
                raise HTTPError(
                    500, "Exception handling function %s:" % f.func_name)
            return ret
        return wrapped
    return handle_exceptions

def get_request_handlers(process, store, handle_exceptions, thread_pool):
    class GetAllUpdatedTracked(BaseAllUpdatedTracked):
        @asynchronous
        @handle_exceptions
        def get(self, sim):
            changelist_str = (
                self.request.body if (
                    self.request.body and store.objectless_server) else None)
            changelist = (
                cbor.loads(changelist_str)
                if changelist_str is not None else
                dict())
            thread_pool.apply_async(
                store.getupdates,
                args=(sim, changelist, self.complete_update))

        @handle_exceptions
        def complete_update(self, sim, data, content_type):
            io_loop = tornado.ioloop.IOLoop.current()
            io_loop.add_callback(self._complete_update, sim, data, content_type)

        @handle_exceptions
        def _complete_update(self, sim, data, content_type):
            self.set_header("content-type", content_type)
            self.write(data)
            self.finish()

    class PostAllUpdatedTracked(BaseAllUpdatedTracked):
        @asynchronous
        @handle_exceptions
        def post(self, sim):
            data = self.request.body
            thread_pool.apply_async(
                store.update, args=(sim, data, self.complete_push))

        @handle_exceptions
        def complete_push(self, sim):
            io_loop = tornado.ioloop.IOLoop.current()
            io_loop.add_callback(self._complete_push, sim)

        @handle_exceptions
        def _complete_push(self, sim):
            self.finish()

    class GetStoreStatus(RequestHandler):
        def get(self, status_name):
            if status_name == "queue_size":
                self.set_header("content-type", "text/plain")
                self.write(str(store.master_dataframe.queue.qsize()))

    class Register(BaseRegisterHandler):
        @handle_exceptions
        def put(self, sim):
            data = self.request.body
            json_dict = json.loads(data)
            typemap = json_dict["sim_typemap"]
            wire_format = (
                json_dict["wire_format"]
                if "wire_format" in json_dict else
                "json")
            wait_for_server = (
                json_dict["wait_for_server"]
                if "wait_for_server" in json_dict else
                False)
            store.register_app(
                sim, typemap, wire_format=wire_format,
                wait_for_server=wait_for_server)

        @handle_exceptions
        def delete(self, sim):
            process.disconnect(sim)

    class GetInvalids(RequestHandler):
        def get(self, app):
            filename = os.path.join(store.INVALIDS, app)
            data = (
                open(filename).read()
                if os.path.exists(filename) else
                "NO INVALIDS DETECTED.")
            self.set_header("content-type", "text/plain")
            self.write(data)

    return GetAllUpdatedTracked, PostAllUpdatedTracked, Register, GetStoreStatus, GetInvalids

def SetupLoggers(debug) :
    if debug:
        logl = logging.INFO
    else:
        logl = logging.INFO

    logger = logging.getLogger()
    logger.setLevel(logl)
    folder = "logs/"
    if not os.path.exists(folder):
        os.mkdir(folder)
    logfile = os.path.join(folder, "frameserver.log")
    flog = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
    flog.setLevel(logl)
    flog.setFormatter(logging.Formatter(
        '%(levelname)s [%(name)s] [%(asctime)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('[%(name)s] [%(asctime)s] %(message)s'))
    if debug:
        clog.setLevel(logging.INFO)
    else:
        clog.setLevel(logging.INFO)
    logger.addHandler(clog)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("tornado.access").setLevel(logging.WARNING)
    return logger


class TornadoServerProcess(Process):
    '''Class that helps launch and maintain Tornado Server.'''

    @property
    def not_ready(self):
        return (
            self.logger is None
            or self.store is None
            or self.timers is None
            or self.app is None)

    def __init__(self):
        super(TornadoServerProcess, self).__init__(
            name="BENCHMARK_TornadoServerProcess")
        self.daemon = True
        self.work_queue = Queue()
        self.done = False
        self.logger = None
        self.store = None
        self.timers = None
        self.app = None
        self.port = None
        self.timeout = 0
        self.disconnect_timer = None
        self.app_thread = Thread(
            target=tornado.ioloop.IOLoop.current().start,
            name="Thread_TornadoServer")
        self.app_thread.daemon = True
        self.start_event = Event()
        self.reset_event = Event()
        self.get_size_queue = Queue()
        self.thread_pool = None

    #################################################
    # APIs not in the same process as run
    #################################################

    def setup(self, debug, store, timeout=0):
        self.work_queue.put(SetUpRequest(debug, store, timeout))

    def start_server(self, port, console):
        stdin = sys.stdin
        self.work_queue.put(StartRequest(
            port, console, stdin))

    def restart_store(self, instrument_filename=None):
        self.work_queue.put(RestartStoreRequest(instrument_filename))

    def shutdown(self):
        self.work_queue.put(ShutdownRequest())

    def wait_for_start(self):
        self.start_event.wait()
        self.start_event.clear()

    def wait_for_reset(self):
        self.reset_event.wait()
        self.reset_event.clear()

    def get_server_queue_size(self):
        self.work_queue.put(GetQueueSizeRequest())
        return self.get_size_queue.get()

    #################################################
    # APIs in the same process as run
    #################################################

    def run(self):
        while not self.done:
            try:
                req = self.work_queue.get()
                if isinstance(req, SetUpRequest):
                    self.process_setup(req)
                elif isinstance(req, StartRequest):
                    self.process_start(req)
                elif isinstance(req, RestartStoreRequest):
                    self.process_restart_store(req)
                elif isinstance(req, ShutdownRequest):
                    self.process_shutdown()
                elif isinstance(req, GetQueueSizeRequest):
                    self.get_size_queue.put(self.store.master_dataframe.queue.qsize())
            except KeyboardInterrupt:
                self.process_shutdown()

    def process_setup(self, req):
        self.logger = SetupLoggers(req.debug)
        self.logger.info("Log level is " + str(self.logger.level))
        self.store = req.store
        self.timers = dict()
        self.timeout = req.timeout
        self.thread_pool = ThreadPool()
        self.thread_pool.daemon = True
        handle_exceptions = get_exception_handler(
            self.timers, self.store, self.logger)
        (get_all_updated_tracked, post_all_updated_tracked,
         register, get_store_status, get_invalids) = (
             get_request_handlers(
                 self, self.store, handle_exceptions, self.thread_pool))
        self.app = tornado.web.Application([
            (r"/([a-zA-Z0-9_-]+)/getupdated", get_all_updated_tracked),
            (r"/([a-zA-Z0-9_-]+)/postupdated", post_all_updated_tracked),
            (r"/([a-zA-Z0-9_-]+)/invalid", get_invalids),
            (r"/([a-zA-Z0-9_-]+)", register),
            (r"/status/([a-zA-Z0-9_-]+)", get_store_status)])

    def process_start(self, req):
        if self.not_ready:
            raise RuntimeError(
                "Trying to start tornado server without setting it up first.")
        self.port = req.port
        self.store.start()

        if req.console:
            console = SpacetimeConsole(self.store, self, stdin=req.stdin)
            con_thread = Thread(
                target=console.cmdloop,
                name="Thread_spacetime_console")
            con_thread.daemon = True
            con_thread.start()

        if self.timeout > 0:
            self.start_timer()

        self.app.listen(self.port)
        self.app_thread.start()
        self.start_event.set()

    def process_restart_store(self, req):
        if self.not_ready:
            raise RuntimeError(
                "Trying to restart tornado server without setting it up first.")
        if req.instrument_filename:
            self.store.save_instrumentation_data(req.instrument_filename)
        self.store.clear()
        self.reset_event.set()

    def process_shutdown(self):
        self.store.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        self.done = True

    def start_timer(self):
        self.disconnect_timer = Timer(self.timeout, self.check_disconnect, ())
        self.disconnect_timer.start()

    def check_disconnect(self):
        if not self.store.pause_servers:
            for sim in self.timers:
                if (time.time() - self.timers[sim]) > self.timeout:
                    self.disconnect(sim)
        self.start_timer()

    def disconnect(self, sim):
        self.store.gc(sim)
        del self.timers[sim]
