from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary


class DataframeChanges_Base(RecursiveDictionary):
    # Add all checks to DataframeChanges structure here.
    def ParseFromDict(self, parsed_dict):
        self.rec_update(parsed_dict)
