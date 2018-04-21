'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.metadata.subset import SubsetMetadata
from rtypes.pcc.this import thisattr
from rtypes.pcc.this import THIS
from rtypes.pcc.attributes import staticmethod_predicate


class subset(object):
    def __init__(self, of_class):
        self.type = of_class

    def __call__(self, actual_class):
        self.resolve_anon_predicate(actual_class, actual_class.__predicate__)
        ofclass_metadata, parent_is_anon = self.resolve_anon_ofclass(
            actual_class)
        addition_categories = (
            actual_class.__rtypes_metadata__.categories
            if parent_is_anon else
            set())

        actual_class.__rtypes_metadata__ = SubsetMetadata(
            actual_class, ofclass_metadata)
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

    def resolve_anon_predicate(self, actual_class, predicate):
        if not isinstance(predicate, staticmethod_predicate):
            return
        new_predicate_dims = list()
        for dim in predicate.dimensions:
            if isinstance(dim, thisattr):
                node = actual_class
                for part in dim.__rtypes_attr_name__.split("."):
                    try:
                        node = getattr(node, part)
                    except AttributeError:
                        raise TypeError(
                            "Couldnt resolve anon dimension at %s" % part)
                if node is not actual_class:
                    new_predicate_dims.append(node)
                else:
                    raise TypeError("Couldnt resolve anon dimension %r", dim)
            else:
                new_predicate_dims.append(dim)
        predicate.dimensions = new_predicate_dims
