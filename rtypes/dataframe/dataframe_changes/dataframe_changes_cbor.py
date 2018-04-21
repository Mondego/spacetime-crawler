from rtypes.dataframe.dataframe_changes.IDataframeChanges import *
import cbor

class DataframeChanges(DataframeChanges_Base):
    def ParseFromString(self, str_value):
        self.ParseFromDict(cbor.loads(str_value))

    def SerializeToString(self):
        return cbor.dumps(self)