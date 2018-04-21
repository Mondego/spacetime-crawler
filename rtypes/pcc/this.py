class thisattr(object):
    @property
    def __rtypes_attr_name__(self):
        return self.__rtypes_attr_name

    def __init__(self, name, prev):
        self.__rtypes_attr_name = (
            (prev.__rtypes_attr_name__ + ".")
            if prev.__rtypes_attr_name__ else
            "") + name
        self.__rtypes_attr_prev = prev
        self.__rtypes_attr_children = dict()

    def __getattribute__(self, arg):
        try:
            return object.__getattribute__(self, arg)
        except AttributeError:
            arg_obj = thisattr(arg, self)
            self.__rtypes_attr_children.setdefault(arg, arg_obj)
            self.arg = arg_obj
            return arg_obj

class thisclass(object):
    @property
    def __rtypes_attr_name__(self):
        return self.__rtypes_attr_name

    def __init__(self):
        self.__rtypes_attr_name = ""
        self.__rtypes_attr_children = dict()

    def __getattribute__(self, arg):
        try:
            return object.__getattribute__(self, arg)
        except AttributeError:
            arg_obj = thisattr(arg, self)
            self.__rtypes_attr_children.setdefault(arg, arg_obj)
            self.arg = arg_obj
            return arg_obj

THIS = thisclass()
