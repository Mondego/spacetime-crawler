from rtypes.pcc.utils.enums import PCCCategories


def impure(cls):
    if not hasattr(cls, "__rtypes_metadata__"):
        raise TypeError("Class {0} is not a PCC class.".format(repr(cls)))
    cls.__rtypes_metadata__.categories.add(PCCCategories.impure)
    return cls
