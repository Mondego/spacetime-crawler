import cmd
import shlex

# pylint: disable=W0613
class SpacetimeConsole(cmd.Cmd):
    """Command console interpreter for frame."""
    prompt = 'Spacetime> '

    def __init__(self, store, server, *args, **kwargs):
        self.store = store
        self.server = server
        cmd.Cmd.__init__(self, *args, **kwargs)

    def do_quit(self, line):
        """ quit
        Exits all applications by calling their shutdown methods.
        """
        self.server.shutdown()
        self.store.shutdown()
        return True

    def do_exit(self, line):
        """ exit
        Exits all applications by calling their shutdown methods.
        """
        self.server.shutdown()
        self.store.shutdown()
        return True

    def do_findobjs(self, line):
        """ findobjs
        Looks for objects where a given dimension matches a given value for a
        given set.
        """
        tokens = shlex.split(line)
        name2class = self.store.master_dataframe.type_manager.name2class
        if len(tokens) == 3:
            type_text = tokens[0]
            dim = tokens[1]
            val = tokens[2]
            if type_text in name2class:
                tp = name2class[type_text]
                if hasattr(tp, dim):
                    objs = self.store.get(tp)
                    for obj in objs:
                        try:
                            value = getattr(obj, dim)
                        except Exception:  # pylint: disable=W0703
                            continue
                        if str(value) == val:
                            if hasattr(obj, dim):
                                print "%s: %s" % (
                                    dim, repr(getattr(obj, dim)))
                else:
                    print "type %s does not have dimension %s" % (
                        type_text, dim)
            else:
                print "could not find type %s" % type_text
        else:
            print "usage: findobjs <type> <dimension> <value>"

    def do_descobj(self, line):
        """ descobj <type> <id>
        Given a type and an id, prints all the dimensions and values.
        Has auto-complete.
        """
        tokens = shlex.split(line)
        name2class = self.store.master_dataframe.type_manager.name2class
        object_map = self.store.master_dataframe.object_manager.object_map
        if len(tokens) ==  2:
            type_text = tokens[0]
            oid = tokens[1]
            if type_text in name2class:
                obj = {}
                try:
                    obj = object_map[type_text][oid]
                except:
                    print "could not find object with id %s" % oid
                for dim in obj.__class__.__rtypes_metadata__.dimensions:
                    print "%s: %s" % (
                        dim._name, repr(getattr(obj, dim._name, None)))
            else:
                print "could not find type %s" % type_text


    def complete_descobj(self, text, line, begidx, endidx):
        tokens = shlex.split(line)
        name2class = self.store.master_dataframe.type_manager.name2class
        object_map = self.store.master_dataframe.object_manager.object_map
        if len(tokens) == 1:
            completions = name2class.keys()
        elif len(tokens) == 2 and text:
            completions = [t for t in name2class.keys() if t.startswith(text)]
        else:
            if tokens[1] in name2class:
                if len(tokens) == 2 and not text and tokens[1] in object_map:
                    completions = object_map[tokens[1]].keys()
                elif len(tokens) == 3 and text and tokens[1] in object_map:
                    completions = [
                        oid
                        for oid in object_map[tokens[1]].keys()
                        if oid.startswith(text)]
            else:
                print "\n%s is not a valid type." % tokens[1]
        return completions

    def do_objsin(self, type_text):
        """ objsin <type>
        Prints the primary key of all objects of a type (accepts auto-complete)
        """
        name2class = self.store.master_dataframe.type_manager.name2class
        if type_text in name2class:
            objs = self.store.get(name2class[type_text])
            if objs:
                print "{0:20s}".format("ids")
                print "============="
                for oid in objs:
                    print "{0:20s}".format(oid)
                print ""
        else:
            print "could not find type %s" % type_text

    def complete_objsin(self, text, line, begidx, endidx):
        name2class = self.store.master_dataframe.type_manager.name2class
        if not text:
            completions = name2class.keys()
        else:
            completions = [t for t in name2class.keys() if t.startswith(text)]
        return completions

    def do_countobjsin(self, type_text):
        """ objsin <type>
        Prints the primary key of all objects of a type (accepts auto-complete)
        """
        name2class = self.store.master_dataframe.type_manager.name2class
        if type_text in name2class:
            objs = self.store.get(name2class[type_text])
            if objs:
                print "============="
                print "Number of objects in %s is %d" % (type_text, len(objs))
                print ""
        else:
            print "could not find type %s" % type_text

    def complete_countobjsin(self, text, line, begidx, endidx):
        name2class = self.store.master_dataframe.type_manager.name2class
        if not text:
            completions = name2class.keys()
        else:
            completions = [t for t in name2class.keys() if t.startswith(text)]
        return completions

    def complete_list(self, text, line, begidx, endidx):
        return ['sets', 'apps']

    def do_list(self, line):
        """ list ['sets','apps']
        list accepts one of two arguments:
        * 'sets' prints all pcc sets tracked by the server
        * 'apps' prints the name of all applications registered with the server
        """
        name2class = self.store.master_dataframe.type_manager.name2class
        if line == "sets":
            for t in name2class.keys():
                print t
        elif line == "apps":
            all_apps = self.store.get_app_list()
            for app in all_apps:
                print app
        else:
            print line

    def do_clear(self, type_text):
        """ clear [<type>, '!all']
        Deletes all objects of the type passed.

        If '!all' is passed, all objects of all types are cleared.
        """
        name2class = self.store.master_dataframe.type_manager.name2class
        if type_text:
            if type_text == "!all":
                self.store.clear()
                print "cleared all objects in store..."
            else:
                try:
                    self.store.clear(name2class[type_text])
                    print "cleared all objects of type %s" % type_text
                except:
                    print "could not clear objects of type %s" % type_text

    def postcmd(self, stop, line):
        if stop:
            print "Shutting down spacetime command prompt."
        return stop

    def emptyline(self):
        pass

    def do_EOF(self, line):
        #print "End of the line"
        #self.server.shutdown()
        #self.store.shutdown()
        return False
