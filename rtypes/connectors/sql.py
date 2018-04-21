import ast
import inspect
import logging
import datetime
import cPickle
from mysql.connector import MySQLConnection
from mysql.connector import errors

from rtypes.pcc.utils.cache import cache
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary
from rtypes.pcc.utils.enums import PCCCategories, Record, Event
from rtypes.pcc.utils._utils import ValueParser

LOGGER = logging.getLogger("rtypes-sql")


class RTypesMySQLConnection(MySQLConnection):
    def __rtypes_query__(self, pcc_types, **parameters):
        self.start_transaction()
        cursor = self.cursor()
        result = RecursiveDictionary()
        try:
            for pcc_type in pcc_types:
                metadata = pcc_type.__rtypes_metadata__
                # if PCCCategories.impure in metadata.categories:
                #     continue
                # if PCCCategories.pcc_set not in metadata.categories:
                #     continue
                dims_order, query = convert_to_read_query(pcc_type)
                cursor.execute(query)
                # metadata = pcc_type.__rtypes_metadata__
                grp_changes = result.setdefault(
                    metadata.groupname, RecursiveDictionary())
                for row in cursor.fetchall():
                    dim_dict = dict(zip(dims_order, row))
                    primarykey_value = dim_dict[metadata.primarykey.name]
                    dim_changes = convert_to_dim_map(dim_dict)
                    obj_changes = grp_changes.setdefault(
                        primarykey_value,
                        RecursiveDictionary())
                    obj_changes.setdefault(
                        "types",
                        RecursiveDictionary())[metadata.name] = Event.New
                    obj_changes.setdefault(
                        "dims", RecursiveDictionary()).rec_update(dim_changes)
        except errors.Error as err:
            self.rollback()
            # LOGGER.error("Exeception %s seen during query", repr(err))
            result = RecursiveDictionary()
        self.commit()
        cursor.close()
        return {"gc": result}, True

    def __rtypes_write__(self, changes, pcc_type_map):
        try:    
            self.start_transaction()
        
            cursor = self.cursor()
            cursor.execute("SHOW TABLES;")
            rows = cursor.fetchall()
            existing_tables = set(row[0] for row in rows if row)
            if "gc" not in changes and "types" not in changes:
                return
            queries = list()
            for typekey, status in changes.setdefault(
                    "types", dict()).iteritems():
                if typekey not in pcc_type_map:
                    raise TypeError(
                        "Could not process unregistered type %s", typekey)
                metadata = pcc_type_map[typekey].__rtypes_metadata__
                if metadata.shortname.lower() in existing_tables:
                    continue
                # if PCCCategories.impure in metadata.categories:
                #     continue
                # if PCCCategories.pcc_set not in metadata.categories:
                #     continue
                if status == Event.New:
                    queries.append(create_table_query(pcc_type_map[typekey]))
                if status == Event.Delete:
                    queries.append(drop_table_query(pcc_type_map[typekey]))

            for group_key, group_changes in changes.setdefault(
                    "gc", dict()).iteritems():
                for oid, obj_changes in group_changes.iteritems():
                    if group_key not in obj_changes["types"]:
                        continue
                    event_type = determine_update_type(
                        group_key, obj_changes["types"])
                    if event_type == Event.New:
                        queries.append(
                            insert_query(
                                group_key, obj_changes["dims"], pcc_type_map))
                    elif event_type == Event.Modification:
                        queries.append(
                            modify_query(
                                group_key, oid,
                                obj_changes["dims"], pcc_type_map))
                    elif event_type == Event.Delete:
                        queries.append(
                            delete_query(group_key, oid, pcc_type_map))
            for q, args in queries:
                # print q, args
                try:
                    cursor.execute(q, args)
                except errors.IntegrityError:
                    continue
            self.commit()
            cursor.close()
        except errors.Error as err:
            self.reconnect()
            self.rollback()
            # print err
            # LOGGER.error("Exeception %s seen during write", repr(err))


def convert_to_dim_map(dim_dict):
    return {
        dim: convert_to_dim_value(value)
        for dim, value in dim_dict.iteritems()}


def convert_to_dim_value(value):
    tp = ValueParser.get_obj_type(value)
    return {
        "type": tp,
        "value": format_value(tp, value)
    }


def format_value(tp, value):
    if tp == Record.DATETIME and not isinstance(value, str):
        return "%d-%d-%d" % (
            value.year, value.month, value.day)
    if tp == Record.DICTIONARY:
        return cPickle.dumps(value)
    if tp == Record.COLLECTION:
        return cPickle.dumps(value)
    return value


def determine_update_type(group_key, type_changes):
    if group_key in type_changes:
        return type_changes[group_key]
    type_changes_seen = set(type_changes.values())
    if type_changes_seen == set([Event.Delete]):
        return Event.Delete
    if type_changes_seen == set([Event.New]):
        # Should not be there without the groupkey.
        LOGGER.warning("Event.New seen without group key being present")
        return Event.New
    if Event.Modification in type_changes_seen:
        return Event.Modification


def insert_query(group_key, dims, pcc_type_map):
    if group_key not in pcc_type_map:
        raise TypeError("Unregistered type %s found in changes", group_key)
    metadata = pcc_type_map[group_key].__rtypes_metadata__
    names, values = zip(*dims.iteritems())
    query = "INSERT INTO {0} ({1}) VALUES ({2});".format(
        metadata.shortname,
        ", ".join(names),
        ", ".join(["%s"] * len(names))
    )

    return query, [format_value(v["type"], v.setdefault("value", None))
                   for v in values]


def modify_query(group_key, oid, dims, pcc_type_map):
    if group_key not in pcc_type_map:
        raise TypeError("Unregistered type %s found in changes", group_key)
    metadata = pcc_type_map[group_key].__rtypes_metadata__
    primarykey_field = metadata.primarykey.name
    names, values = zip(*dims.iteritems())
    query = (
        "UPDATE {0} SET "
        + (", ".join(name + " = %s" for name in names))
        + " WHERE {1} = %s;").format(
            metadata.shortname,
            primarykey_field)

    return query, [format_value(v["type"], v.setdefault("value", None))
                   for v in values] + [oid]


def delete_query(group_key, oid, pcc_type_map):
    if group_key not in pcc_type_map:
        raise TypeError("Unregistered type %s found in changes", group_key)
    metadata = pcc_type_map[group_key].__rtypes_metadata__
    primarykey_field = metadata.primarykey.name
    query = (
        "DELETE FROM {0} WHERE {1} = (%s);".format(
            metadata.shortname, primarykey_field))
    return query, [oid]


@cache
def convert_to_read_query(pcc_type):
    metadata = pcc_type.__rtypes_metadata__
    names = [dim.name for dim in metadata.dimensions]
    primarykey = metadata.primarykey
    select_filters = read_filters(pcc_type)
    return (names, "SELECT {0} FROM {1} {2};".format(
        ", ".join(names),
        metadata.shortname,
        select_filters))


def create_obj(sql_obj, dims_order, pcc_type):
    obj = _container()
    obj.__class__ = pcc_type
    for i in range(len(dims_order)):
        setattr(obj, dims_order[i], sql_obj[i])
    return obj.__primarykey__, obj


def create_table_query(entity):
    metadata = entity.__rtypes_metadata__
    if metadata.final_category is PCCCategories.pcc_set:
        query = (
            ("CREATE TABLE %s (" % (metadata.shortname,))
            + ", ".join([
                " ".join([d.name,
                          convert_type(
                              d.type, primarykey=(d == metadata.primarykey)),
                          "PRIMARY KEY" if d == metadata.primarykey else ""])
                for d in metadata.dimensions])
            + ");")
        return query, list()
    else:
        # TODO: Make this work for all types of alternate views.
        select_filters = read_filters(entity)
        query = (("CREATE VIEW %s AS SELECT %s FROM %s %s;") %
                 (metadata.shortname,
                  ", ".join(d.name for d in metadata.dimensions),
                  metadata.parent.shortname,
                  select_filters))
        return query, list()


def drop_table_query(entity):
    metadata = entity.__rtypes_metadata__
    tbltype = ("TABLE"
               if metadata.final_category is PCCCategories.pcc_set else
               "VIEW")
    return "DROP %s %s;" % (
        tbltype, entity.__rtypes_metadata__.shortname), list()


def read_filters(tp):
    metadata = tp.__rtypes_metadata__
    filter_str = ""
    if hasattr(metadata, "predicate") and metadata.predicate:
        filter_str += "WHERE " + convert_expr(
            metadata.predicate, metadata.is_new_type_predicate)
    # Have to implement all the groupby and orderby and all that.
    return filter_str


def cleanup(code):
    line1 = code.split("\n")[0]
    cleanline1 = line1.strip()
    starting_tab = len(line1) - len(cleanline1)
    return "\n".join([l[starting_tab:] for l in code.split("\n")])


def convert_expr(func, is_new_type_predicate):
    if is_new_type_predicate:
        func = func.func
    clean_line = cleanup(inspect.getsource(func))

    tree = ast.parse(clean_line)
    return_obj = tree.body[0].body[0].value
    if not is_new_type_predicate:
        obj_varname = tree.body[0].args.args[0].id
        return sqlify(return_obj, obj_varname=obj_varname)
    else:
        dim_names = {dim.id: dim.id for dim in tree.body[0].args.args}
        return sqlify(return_obj, parsed_expr=dim_names)


def sqlify(expr, obj_varname="", parsed_expr=dict()):
    if isinstance(expr, ast.Compare):
        return " ".join([
            sqlify(expr.left, obj_varname, parsed_expr),
            sqlify(expr.ops, obj_varname, parsed_expr),
            sqlify(expr.comparators, obj_varname, parsed_expr)])
    if isinstance(expr, ast.BinOp):
        return " ".join([
            sqlify(expr.left, obj_varname, parsed_expr),
            sqlify(expr.op, obj_varname, parsed_expr),
            sqlify(expr.right, obj_varname, parsed_expr)])
    if isinstance(expr, ast.Attribute):
        return expr.attr
    if isinstance(expr, list):
        return " ".join([sqlify(e, obj_varname, parsed_expr) for e in expr])
    if isinstance(expr, ast.Name):
        if expr.id == "True":
            return '1'
        if expr.id == "False":
            return '0'
        if expr.id == "None":
            return 'NULL'
        return parsed_expr[expr.id] if expr.id in parsed_expr else ""
    if isinstance(expr, ast.Eq):
        return "="
    if isinstance(expr, ast.NotEq):
        return "!="
    if isinstance(expr, ast.Lt):
        return "<"
    if isinstance(expr, ast.LtE):
        return "<="
    if isinstance(expr, ast.Gt):
        return ">"
    if isinstance(expr, ast.GtE):
        return ">="
    if isinstance(expr, ast.Is):
        return "=="
    if isinstance(expr, ast.IsNot):
        return "!="
    if isinstance(expr, ast.Add):
        return "+"
    if isinstance(expr, ast.And):
        return "AND"
    if isinstance(expr, ast.Or):
        return "OR"
    if isinstance(expr, ast.Sub):
        return "-"
    if isinstance(expr, ast.Mult):
        return "*"
    if isinstance(expr, ast.Div):
        return "/"
    if isinstance(expr, ast.Mod):
        return "%"
    if isinstance(expr, ast.Num):
        return str(expr.n)
    if isinstance(expr, ast.Str):
        return expr.s


def convert_type(tp, primarykey=False):
    if tp == int:
        return "INTEGER"
    if tp == float:
        return "REAL"
    if tp == bool:
        return "INTEGER"
    if tp == str and primarykey:
        return "VARCHAR(1000)"
    if tp == str and not primarykey:
        return "TEXT"
    if tp == datetime.date:
        return "DATETIME"
    if tp == dict or tp == RecursiveDictionary:
        return "TEXT"
    if tp == list:
        return "TEXT"


class _container(object):
    pass
