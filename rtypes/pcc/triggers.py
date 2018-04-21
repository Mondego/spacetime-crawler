
class TriggerTime(object):
    before = "b"
    after = "a"


class TriggerAction(object):
    create = "c"
    read = "r"
    update = "u"
    delete = "d"


class TriggerProcedure(object):
    def __init__(self, procedure, pcc_type, time, action, priority):
        self.procedure = procedure
        self.pcc_type = pcc_type
        self.time = time
        self.action = action
        self.priority = priority

    def __call__(self, dataframe=None, new=None, old=None, current=None):
        return self.procedure(dataframe, new, old, current)

    def __eq__(self, v):
        if isinstance(v, int):
            return self.priority == v
        return  self.priority == v.priority
        

    def __lt__(self, v):
        if isinstance(v, int):
            return self.priority < v
        return self.priority < v.priority

    def __gt__(self, v):
        if isinstance(v, int):
            return self.priority > v
        return self.priority > v.priority

class trigger(object):

    def __init__(self, pcc_type, time, action, priority=None):
        self.pcc_type = pcc_type
        self.time = time
        self.action = action
        self.priority = priority

    def __call__(self, procedure):
        return TriggerProcedure(procedure, self.pcc_type, self.time, self.action, self.priority)


class BlockAction(Exception):
    pass


if __name__ == "__main__":
    pass

