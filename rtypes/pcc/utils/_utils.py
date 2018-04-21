import datetime
from dateutil import parser

from rtypes.pcc.utils.enums import Record
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary


class ValueParser(object):
    @staticmethod
    def create_fake_class():
        class container(object):
            pass
        return container

    @staticmethod
    def get_obj_type(obj):
        # both iteratable/dictionary + object type is messed up. Won't work.
        try:
            if hasattr(obj, "__rtypes_metadata__"):
                return Record.FOREIGN_KEY
            if isinstance(obj, dict):
                return Record.DICTIONARY
            if hasattr(obj, "__iter__"):
                return Record.COLLECTION
            if isinstance(obj, int) or isinstance(obj, long):
                return Record.INT
            if isinstance(obj, float):
                return Record.FLOAT
            if isinstance(obj, str) or isinstance(obj, unicode):
                return Record.STRING
            if isinstance(obj, bool):
                return Record.BOOL
            if obj == None:
                return Record.NULL
            if (isinstance(obj, datetime.date)
                    or isinstance(obj, datetime.datetime)):
                return Record.DATETIME
            if hasattr(obj, "__dict__"):
                return Record.OBJECT
        except TypeError as e:
            return -1
        return -1

    @staticmethod
    def parse(record):
        if record["type"] == Record.INT:
            # the value will be in record["value"]
            return long(record["value"])
        if record["type"] == Record.FLOAT:
            # the value will be in record["value"]
            return float(record["value"])
        if record["type"] == Record.STRING:
            # the value will be in record["value"]
            return record["value"]
        if record["type"] == Record.BOOL:
            # the value will be in record["value"]
            return record["value"]
        if record["type"] == Record.NULL:
            # No value, just make it None
            return None

        if record["type"] == Record.OBJECT:
            # The value is {
            #    "omap": <Dictionary Record form of the object (__dict__)>,
            #    "type": {"name": <name of type,
            #             "type_pickled": pickled type <- optional part
            #  }

            # So parse it like a dict and update the object dict
            new_record = RecursiveDictionary()
            new_record["type"] = Record.DICTIONARY
            new_record["value"] = record["value"]["omap"]

            dict_value = ValueParser.parse(new_record)
            value = ValueParser.create_fake_class()()
            # Set type of object from record.value.object.type. Future work.
            value.__dict__ = dict_value
            return value
        if record["type"] == Record.COLLECTION:
            # Assume it is list, as again, don't know this type
            # value is just list of records
            return [
                ValueParser.parse(rec)
                for rec in record["value"]]
        if record["type"] == Record.DICTIONARY:
            # Assume it is dictionary, as again, don't know this type
            # value-> [{"k": key_record, "v": val_record}]
            # Has to be a list because keys may not be string.
            return RecursiveDictionary([
                (ValueParser.parse(p["k"]), ValueParser.parse(p["v"]))
                for p in record["value"]])
        if record["type"] == Record.DATETIME:
            return parser.parse(record["value"])
