'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.metadata.set import SetMetadata


def pcc_set(actual_class):
    actual_class.__rtypes_metadata__ = SetMetadata(actual_class)
    return actual_class
