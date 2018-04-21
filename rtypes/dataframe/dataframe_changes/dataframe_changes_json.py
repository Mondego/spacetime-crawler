from rtypes.dataframe.dataframe_changes.IDataframeChanges import *
import json

class DataframeChanges(DataframeChanges_Base):
    def ParseFromString(self, str_value):
        self.ParseFromDict(json.loads(str_value))

    def SerializeToString(self):
        return json.dumps(self)