'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.metadata.join import JoinMetadata
from rtypes.pcc.attributes import namespace_property

class join(object):
    def __init__(self, **classes):
        # List of classes that are part of join
        # should create a class when it gets called
        self.namespace_map = classes

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        nprops, flattened_props, parents = self.build_namespaces(actual_class)
        actual_class.__rtypes_metadata__ = JoinMetadata(
            actual_class, parents, nprops, flattened_props)
        return actual_class

    def build_namespaces(self, actual_class):
        flattened = list()
        nprops = list()
        parents = list()
        for name, jclass in self.namespace_map.iteritems():
            nprop = namespace_property(name, jclass)
            flattened.extend(nprop.__rtypes_property_dimensions__)
            setattr(actual_class, name, nprop)
            nprops.append(nprop)
            parents.append(jclass.__rtypes_metadata__)
        return nprops, flattened, parents
