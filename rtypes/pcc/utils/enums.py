from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary

class PCCCategories(object):
    pcc_set = 1
    subset = 2
    join = 3
    projection = 4
    union = 5
    intersection = 6
    parameter = 7
    impure = 8
    unknown_type = 9


class Event(object):
    Delete = 0
    New = 1
    Modification = 2


class Record(RecursiveDictionary):
    INT = 1
    FLOAT = 2
    STRING = 3
    BOOL = 4
    NULL = 5

    COLLECTION = 10
    DICTIONARY = 11

    OBJECT = 12
    FOREIGN_KEY = 13

    DATETIME = 14