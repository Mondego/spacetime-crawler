from multiprocessing import RLock
from rtypes.pcc.utils.enums import PCCCategories

type_lock = RLock()
object_lock = RLock()


class DataframeType(object):
    # Name -> str,
    # Type -> type,
    # GroupType -> type,
    # GroupKey -> key,
    # Category -> dict,
    # ClosestSaveableParent -> DataframeType,
    # GroupMembers -> [DataframeType]
    # IsPure -> Bool

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, obj):
        if isinstance(obj, (str, unicode)):
            return self.name == obj
        return self.name == obj.name

    @property
    def can_be_persistent(self):
        return self.saveable_parent != None

    @property
    def has_params(self):
        return len(self.parameter_types) != 0

    def __init__(
            self, tp, grp_tp, categories, depends=list(), saveable_parent=None,
            group_members=set(), pure_group_members=set(), is_pure=False,
            parameter_types=dict(), super_class=None, observable=True):
        self.metadata = tp.__rtypes_metadata__
        self.type = tp
        self.group_type = grp_tp
        self.categories = categories
        self.depends = depends
        self.saveable_parent = saveable_parent
        self.group_members = group_members
        self.pure_group_members = pure_group_members
        self.is_pure = is_pure
        self.parameter_types = parameter_types
        self.super_class = super_class
        self.observable = observable
        self.name = self.metadata.name
        self.group_key = self.metadata.groupname
        self.is_base_type = self.name == self.group_key
        self.is_projection = PCCCategories.projection in self.categories
        self.dim_to_predicate_map = dict()
