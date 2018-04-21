'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.metadata.projection import ProjectionMetadata
from rtypes.pcc.this import thisattr, THIS


class projection(object):
    def __init__(self, of_class, *dimensions):
        # Class that it is going to be a projection of.
        self.type = of_class
        self.dimensions = dimensions

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        parent_metadata, parent_is_anon = self.resolve_anon_ofclass(
            actual_class)
        self.resolve_anon_dimensions(actual_class)

        addition_categories = (
            actual_class.__rtypes_metadata__.categories
            if parent_is_anon else
            set())

        actual_class.__rtypes_metadata__ = ProjectionMetadata(
            actual_class, parent_metadata, self.dimensions)
        actual_class.__rtypes_metadata__.categories.update(addition_categories)
        return actual_class

    def resolve_anon_ofclass(self, actual_class):
        is_anon_class = False
        if self.type is THIS:
            self.type = actual_class
            is_anon_class = True
        if hasattr(self.type, "__rtypes_metadata__"):
            return self.type.__rtypes_metadata__, is_anon_class
        raise TypeError("Subset has to be built on a type that is a PCC type")

    def resolve_anon_dimensions(self, actual_class):
        new_dims = list()
        for dim in self.dimensions:
            if isinstance(dim, thisattr):
                node = actual_class
                for part in dim.__rtypes_attr_name__.split("."):
                    try:
                        node = getattr(node, part)
                    except AttributeError:
                        raise TypeError(
                            "Couldnt resolve anon dimension at %s" % part)
                if node is not actual_class:
                    new_dims.append(node)
                else:
                    raise TypeError("Couldnt resolve anon dimension %r", dim)
            else:
                new_dims.append(dim)
        self.dimensions = new_dims
