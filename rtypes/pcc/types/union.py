'''
Create on Feb 27, 2016

@author: Rohan Achar
'''


class union(object):
    def __init__(self, *types):
        # Classes that it is going to be a union of.
        self.types = types

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        build_required_attrs(
            actual_class, PCCCategories.union, self.types)
        return actual_class