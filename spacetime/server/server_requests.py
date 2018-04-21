class ServerRequest(object):
    pass


class SetUpRequest(ServerRequest):
    def __init__(self, debug, store, timeout):
        self.debug = debug
        self.store = store
        self.timeout = timeout


class StartRequest(ServerRequest):
    def __init__(self, port, console, stdin):
        self.port = port
        self.console = console
        self.stdin = stdin


class RestartStoreRequest(ServerRequest):
    def __init__(self, instrument_filename=None):
        self.instrument_filename = instrument_filename


class ShutdownRequest(ServerRequest):
    pass

class GetQueueSizeRequest(ServerRequest):
    pass
