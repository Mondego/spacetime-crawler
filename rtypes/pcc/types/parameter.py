'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.utils.enums import PCCCategories


class ParameterMode(object):
    # default is collection
    Singleton = "singleton"
    Collection = "collection"


class parameter(object):
    def __init__(self, *types, **kwargs):
        self._types = types
        self._mode = (kwargs["mode"]
                      if "mode" in kwargs else
                      ParameterMode.Collection)

    def __call__(self, pcc_class):
        # parameter should be on pcc classes only
        if len(pcc_class.mro()) < 2:
            raise TypeError("Parameter type must derive from some type")
        if not hasattr(pcc_class, "__rtypes_metadata__"):
            raise TypeError("Parameter type must be on a PCC class")
        metadata = pcc_class.__rtypes_metadata__
        metadata.parameter_types.setdefault(
            self._mode, list()).extend(self._types)
        metadata.categories.add(PCCCategories.parameter)
        return pcc_class
