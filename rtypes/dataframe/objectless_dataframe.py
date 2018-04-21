'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import
from uuid import uuid4

from rtypes.dataframe.state_manager import StateManager

BASE_TYPES = set([])


class ObjectlessDataframe(object):
    def __init__(self, name=str(uuid4()), maintain_change_record=True):
        # Unique ID for this dataframe.
        self.name = name

        # The object that deals with object management
        self.state_manager = StateManager(maintain_change_record)

    def add_types(self, types):
        self.state_manager.add_types(types)

    def add_type(self, tp):
        self.state_manager.add_type(tp)

    def apply_changes(self, changes, except_app=None):
        return self.state_manager.apply_changes(changes, except_app)

    def get_record(self, changelist=None, app=None):
        return self.state_manager.get_records(changelist, app)
