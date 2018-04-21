def xml(entity):
    entity.__original_representation__ = xmls_to_objs(entity)
    return entity

class _container(object):
    pass

def xmls_to_objs(entity):
    @staticmethod
    def xml_to_entity_objs(objs):
        new_objs = list()
        cls_name = entity.__realname__
        dimension_names = entity.__dimensions__
        for xml_obj in objs.getElementsByTagName(cls_name):
            new_obj = _container()
            new_obj.__class__ = entity
            for dim in dimension_names:
                dim_value = xml_obj.getElementsByTagName(dim.name)[0]
                setattr(new_obj, dim.name, dim.type(dim_value.firstChild.nodeValue))
            new_objs.append(new_obj)
        return new_objs
    return xml_to_entity_objs


class xmlpath(object):
    def __init__(self, node_name):
        self.__node_name = node_name

    def __call__(self, entity):
        entity.__XML__ = self.__node_name
        entity.__original_representation__ = "XML"
