from spacetime.server.server_process import TornadoServerProcess

class SpacetimeLauncher(object):
    def __init__(self, store, args=None, config=None):
        self.port = 12000
        self.profile = False
        self.debug = False
        self.trackip = False
        self.timeout = 0
        self.clearempty = False
        self.objectless_dataframe = False
        if config:
            self.load_config_from_dict(config)
        if args:
            self.load_config_from_args(args)
        self.store = store
        self.server = TornadoServerProcess()
        self.server.setup(self.debug, self.store, self.timeout)


    def load_config_from_args(self, args):
        self.port = args.port
        self.profile = args.profile
        self.debug = args.debug
        self.trackip = args.trackip
        self.timeout = args.timeout
        self.clearempty = args.clearempty
        self.objectless_dataframe = not args.object

    def load_config_from_dict(self, config):
        self.port = config.setdefault("port", self.port)
        self.profile = config.setdefault("profile", self.profile)
        self.debug = config.setdefault("debug", self.debug)
        self.trackip = config.setdefault("trackip", self.trackip)
        self.timeout = config.setdefault("timeout", self.timeout)
        self.clearempty = config.setdefault("clearempty", self.clearempty)
        self.objectless_dataframe = config.setdefault(
            "objectless", self.objectless_dataframe)

    def start(self, console=False):
        self.server.start()
        self.server.start_server(self.port, console)

    def shutdown(self):
        self.server.shutdown()

    def clear_store(self, instrument_filename=None):
        self.server.restart_store(instrument_filename)

    def join(self):
        self.server.join()

    def wait_for_start(self):
        self.server.wait_for_start()

    def wait_for_reset(self):
        self.server.wait_for_reset()
    
    def get_queue_size(self):
        return self.server.get_server_queue_size()

def start_server(store, args=None, config=None, console=False):
    if not args and not config:
        raise RuntimeError("One of args or configfile has to be set")
    spacetime_launcher = SpacetimeLauncher(
        store, args, config)
    spacetime_launcher.start(console)
    return spacetime_launcher
