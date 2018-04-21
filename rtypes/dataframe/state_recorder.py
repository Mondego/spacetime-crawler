class StateRecorder(object):
    def __init__(self, typename, maintain=True):
        self.typename = typename
        self.obj_to_state = dict()
        self.maintain = maintain

    def __getitem__(self, key):
        return self.obj_to_state[key].full_version

    def iteritems(self):
        return [
            (oid, state.full_version)
            for oid, state in self.obj_to_state.iteritems()]

    def check_oid_exists(self, oid):
        if not self.has_obj(oid):
            raise RuntimeError("Object %r not found" % oid)

    def add_next_change(self, oid, version, changes, marker):
        self.check_oid_exists(oid)
        self.obj_to_state[oid].add_next_change(version, changes, marker)

    def add_transformation(self, oid, version, transform):
        self.check_oid_exists(oid)
        self.obj_to_state[oid].add_transformation(version, transform)

    def get_dim_changes_since(self, oid, prev_version, marker):
        self.check_oid_exists(oid)
        return self.obj_to_state[oid].get_dim_changes_since(
            prev_version, marker)

    def lastkey(self, oid):
        self.check_oid_exists(oid)
        return self.obj_to_state[oid].lastkey()

    def delete_obj(self, oid):
        self.check_oid_exists(oid)
        del self.obj_to_state[oid]

    def add_obj(self, oid, version, full_changes, marker):
        if self.has_obj(oid):
            raise RuntimeError("Adding object that is already present %r" % oid)
        self.obj_to_state[oid] = State(
            version, full_changes, marker, self.maintain)

    def has_obj(self, oid):
        return oid in self.obj_to_state

    def get_full_obj(self, oid, marker):
        self.check_oid_exists(oid)
        return self.obj_to_state[oid].get_full_obj(marker)

class State(object):
    def __init__(self, version, full_changes, marker, maintain=True):
        self.tail = version
        self.head = version
        self.changes = dict()
        self.transforms = dict()
        self.changes[version] = {
            "version": version,
            "changes": full_changes,
            "prev_version": None,
            "next_version": None
        }
        self.full_version = full_changes
        if maintain:
            self.state_to_marker = {self.head: set([marker])}
            self.marker_to_state = {marker: self.head}
        self.maintain_state = maintain

    def maintain_changes(self, marker):
        if self.maintain_state:
            self.state_to_marker.setdefault(self.head, set()).add(marker)
            old_state = self.marker_to_state.setdefault(marker, None)
            if old_state is not None and old_state != self.head:
                self.state_to_marker[old_state].remove(marker)
                if len(self.state_to_marker[old_state]) == 0:
                    del self.state_to_marker[old_state]
            self.marker_to_state[marker] = self.head

            self.maintain()

    def get_full_obj(self, marker):
        yield self.full_version
        self.maintain_changes(marker)

    def lastkey(self):
        return self.head

    def get_dim_changes_since(self, version, marker):
        if version is None:
            yield self.full_version

        if version in self.changes:
            point = self.changes[version]
            while point["next_version"] is not None:
                point = self.changes[point["next_version"]]
                yield point["changes"]
        else:
            if version in self.transforms:
                transforms = self.transforms[version]
                next_tp = transforms["next_timestamp"]
                yield transforms["transform"]
                for next_change in self.get_dim_changes_since(next_tp, marker):
                    yield next_change

        if version is not self.head:
            self.maintain_changes(marker)

    def add_transformation(self, version, transform):
        self.transforms[version] = transform

    def add_next_change(self, version, change, marker):
        prev_head = self.head
        prev_change = self.changes[prev_head]
        prev_change["next_version"] = version

        self.head = version
        self.full_version = State.merge(self.full_version, change)
        self.changes[self.head] = {
            "version": version,
            "changes": change,
            "prev_version": prev_head,
            "next_version": None
        }

        self.maintain_changes(marker)

    def maintain(self):
        current = self.tail
        merge_change = None
        while current is not None:
            change = self.changes[current]
            if current not in self.state_to_marker:
                if merge_change is None:
                    merge_change = change
                else:
                    del self.changes[merge_change["version"]]
                    merge_change["changes"] = State.merge(
                        merge_change["changes"], change["changes"])
                    merge_change["version"] = current
            elif merge_change is not None:
                # pylint: disable=E1137,E1136
                del self.changes[merge_change["version"]]
                merge_change["changes"] = State.merge(
                    merge_change["changes"], change["changes"])
                merge_change["version"] = current
                merge_change["next_version"] = change["next_version"]
                self.changes[current] = merge_change
                if merge_change["prev_version"] is not None:
                    self.changes[
                        merge_change["prev_version"]]["next_version"] = (
                            current)
                else:
                    self.tail = current
                # pylint: enable=E1137,E1136
                merge_change = None
            current = change["next_version"]

    @staticmethod
    def merge(*changes):
        result = {"dims": dict()}
        for change in changes:
            result["dims"].update(change.setdefault("dims", dict()))
        if result["dims"]:
            return result
        return dict()
