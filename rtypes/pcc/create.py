def create(tp, *args, **kwargs):
    if not isinstance(tp, type):
        raise SyntaxError("%s is not a type" % tp)
    if len(args) < 1:
        raise SyntaxError("No objects of type %s" % tp.__name__)
    if not hasattr(tp, "__rtypes_metadata__"):
        raise TypeError(
            "Cannot create non PCC collections ({0})".format(repr(tp)))
    return __create_pcc(tp, args)

def __create_pcc(actual_class, collections):
    metadata = actual_class.__rtypes_metadata__
    collection_map = __build_collection_map(collections)
    return metadata.build_obj_from_collection(collection_map)

def __build_collection_map(collections):
    collection_map = dict()
    for collection in collections:
        if not collection:
            raise RuntimeError("Found empty collection, cannot detect its type")
        metadata = collection[0].__class__.__rtypes_metadata__
        for obj in collection:
            if obj.__class__.__rtypes_metadata__ is not metadata:
                raise RuntimeError("Cannot build pccs using mixed collections.")
        collection_map.setdefault(metadata, list()).extend(collection)
    return collection_map
