from rtypes.dataframe.dataframe_changes.IDataframeChanges import *
import bson

class DataframeChanges(DataframeChanges_Base):
    def ParseFromString(self, str_value):
        self.ParseFromDict(bson.loads(str_value))

    def SerializeToString(self):
        return bson.dumps(self)