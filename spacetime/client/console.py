import cmd
import sys

class SpacetimeConsole(cmd.Cmd):
    """Command console interpreter for frame."""

    def do_exit(self, _):
        """ exit
        Exits all applications by calling their shutdown methods.
        """
        self.shutdown()

    def do_quit(self, _):
        """ quit
        Exits all applications by calling their shutdown methods.
        """
        self.shutdown()

    def do_stop(self, _):
        """ stop
        Stops all applications, but does not exit prompt.
        """
        for frame in self.framelist:
            frame._stop()  # pylint: disable=W0212

    def do_restart(self, _):
        """ restart
        Restarts all frames
        """
        for f in self.framelist:
            f.run_async()

    def emptyline(self):
        pass

    def do_EOF(self, _):
        self.shutdown()

    def __init__(self, framelist, *args, **kwargs):
        self.framelist = framelist
        cmd.Cmd.__init__(self, *args, **kwargs)

    def shutdown(self):
        print "Shutting down all applications..."
        threads = []
        for frame in self.framelist:
            frame._stop()  # pylint: disable=W0212
            threads.append(frame.thread)

        _ = [t.join() for t in threads if t]
        sys.exit(0)
