import bisect

from rtypes.pcc.triggers import TriggerAction, TriggerTime

class TriggerManager(object):
    """Used to regulate all trigger's that exist in the dataframe.
       Can do the following:
       - Add triggers
       - Get triggers
       - Delete triggers
       - Execute triggers

    Attributes:
        trigger_map (dict): Dictionary used to map out all triggers in manager.
            Format: {pcc_type: {time + action: [TriggerProcedures]}}
    """

    def __init__(self):
        """Creates a TriggerManager object, and initializes a trigger_map.
        """
        self.trigger_map = dict()
        self.__rtypes_current_triggers__ = dict()

    #################################################
    ### API Methods #################################
    #################################################

    def add_trigger(self, trigger_obj):
        """Method used to add a single TriggerProcedure obj into the trigger_map.
           If the object is new, then it add's a dict to the map. Else, it will add
           the respective values to existing dict in the map.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to add the trigger into the manager

            Returns:
                None: Does not return anything, simply adds the TriggerProcedure object
                      into the dataframe
        """
        self.__add_trigger(trigger_obj)

    def add_triggers(self, trigger_objs):
        """Method used to add multiple TriggerProcedure objs into the trigger_map.
           If the object is new, then it add's a dict to the map. Else, it will add
           the respective values to existing dict in the map.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to add the trigger into the manager

            Returns:
                None: Does not return anything, simply adds the TriggerProcedure object
                      into the dataframe
        """
        for obj in trigger_objs:
            self.__add_trigger(obj)

    def execute_trigger(self, tp_obj, time, action, dataframe, new, old, current):
        """Method used to execute one specific TriggerProcedure obj.
           Only executes TriggerProcedure objs that meet the specified criteria.
           Passes in arguments "dataframe", "new", "old", and "current" into the procedure

            Args:
                tp (PCC Type): Used to determine if the type has an trigger attached to it
                time (str): Used to specify the activation time of the trigger
                action (str): Used to specify the activation action of the trigger
                dataframe (???): n/a
                new (???): n/a
                old (???): n/a
                current (???): n/a

            Returns:
                None: Does not return anything, only activates procedure objects
        """
        # What arguments would I need in order to allow the trigger to execute ???????
        self.__execute_trigger(tp_obj, time, action, dataframe, new, old, current)
        return True # CHANGE THIS WHEN DONE TESTING ??????????????????????????????????????

    def remove_trigger(self, trigger_obj):
        """Method used to remove TriggerProcedure objs from the trigger_map.
           Preventing the procedure from being activated.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to determine if the TriggerProcedure exist in
                    the trigger_map. Then used to delete the TriggerProcedure.

            Returns:
                None: Does not return anything, only deletes TriggerProcedure obj
        """
        self.__remove_trigger(trigger_obj)

    def trigger_exists(self, tp_obj, time, action):
        return self.__trigger_in_map(tp_obj, time, action)

    #################################################
    ### Private Methods #############################
    #################################################

    def __add_trigger(self, trigger_obj):
        """Method used to add a single TriggerProcedure obj into the trigger_map.
           If the object is new, then it add's a dict to the map. Else, it will add
           the respective values to existing dict in the map.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to add the trigger into the manager

            Returns:
                None: Does not return anything, simply adds the TriggerProcedure object
                      into the dataframe
        """
        if (self.__is_a_unique_trigger(trigger_obj)):
            bisect.insort(
                self.trigger_map.setdefault(
                    trigger_obj.pcc_type,
                    dict()).setdefault(
                        trigger_obj.time + trigger_obj.action,
                        list()),
                trigger_obj)

    def __get_trigger(self, tp, time, action):
        """Method used to get a TriggerProcedure obj attached to a PCC Type.

            Args:
                tp (PCC Type): Used to determine if the type has an trigger attached to it
                time (str): Used to specify the activation time of the trigger
                action (str): Used to specify the activation action of the trigger

            Returns:
                list: This is a list of TriggerProcedure objs that are associated with
                      the specified type and activated at the specified time + action
        """
        if tp in self.trigger_map and ((time + action) in self.trigger_map[tp]):
        # 1a: Check if the pcc_type has any triggers atteched to it
            self.trigger_map[tp][time + action]
            return self.trigger_map[tp][time + action]
        else:
        # 1b: Return an empty list
            return list()

    def __execute_trigger(self, tp, time, action, dataframe, new, old, current):
        """Method used to execute speciific TriggerProcedure objs.
           Only executes TriggerProcedure objs that meet the specified criteria.
           Passes in arguments "dataframe", "new", "old", and "current" into the procedure

            Args:
                tp (PCC Type): Used to determine if the type has an trigger attached to it
                time (str): Used to specify the activation time of the trigger
                action (str): Used to specify the activation action of the trigger
                dataframe (???): n/a
                new (???): n/a
                old (???): n/a
                current (???): n/a

            Returns:
                None: Does not return anything, only activates procedure objects
            __rtypes_current_triggers__ = {"before_update": [Customer, Transaction]}
                """
        for procedure in self.__get_trigger(tp, time, action):
            """
            The process below is used to prevent update triggers from creating infinite
            recursive loops.

            There is a map that is used to do this.

            The map's keys are each procedure object, each key has a set of objets.

            Each set is a literal set of object.r

            These objects show that the procedure has activated on the objects inside
                the set.

            If there isn't an object in the set, then the trigger hasn't neen activated
                on it and is free to do so
            """
            # update
            if action == TriggerAction.update and (new or old or current):

                # if the triggers has been mapped to an object
                if procedure in self.__rtypes_current_triggers__:

                    # If the object is in this map, then dont do anything
                    if new in self.__rtypes_current_triggers__[procedure]:
                        return
                    # Add the object to the map so there isn't recursion
                    # also execute the trigger
                    else:
                        self.__rtypes_current_triggers__[procedure].add(new)
                        procedure(dataframe=dataframe, new=new, old=old, current=current)
                # map the trigger in the dictionary
                else:
                    self.__rtypes_current_triggers__[procedure] = set([new])
                    procedure(dataframe=dataframe, new=new, old=old, current=current)

            # create, read, delete
            else:
                procedure(dataframe=dataframe, new=new, old=old, current=current)

    def __remove_trigger(self, trigger_obj):
        """Method used to remove TriggerProcedure objs from the trigger_map.
           Preventing the procedure from being activated.

            Args:
                trigger_obj (TriggerProcedure):
                    This object is used to determine if the TriggerProcedure exist in
                    the trigger_map. Then used to delete the TriggerProcedure.

            Returns:
                None: Does not return anything, only deletes TriggerProcedure obj
        """
        # If the trigger exist in the trigger_map, remove the Procedure from the map
        index = self.__get_trigger_index(trigger_obj)
        if index != -1:
            self.trigger_map[
                trigger_obj.pcc_type][
                    trigger_obj.time + trigger_obj.action].pop(index)

    def __trigger_obj_in_map(self, trigger_obj):
        """Method determines if the TriggerProcedure obj exist in the trigger_map.

                trigger_obj (TriggerProcedure):
                    Used to determine if the TriggerProcedure obj exist in the trigger_map

            Returns:
                bool: True if the TriggerProcedure exist in the trigger_map, else False
        """
        return self.__trigger_in_map(
            trigger_obj.pcc_type, trigger_obj.time, trigger_obj.action)

    def __trigger_in_map(self, tp, time, action):
        try:
            return len(self.trigger_map[tp][time + action]) != 0
        except KeyError:
            return False

    def __is_a_unique_trigger(self,trigger_obj):
        # If it is -1, that means it isn't in the trigger map yet!
        return not trigger_obj in self.trigger_map

    def __get_trigger_index(self, trigger_obj):
        trigger_list = (
            self.trigger_map[
                trigger_obj.pcc_type][trigger_obj.time + trigger_obj.action])
        index_before = bisect.bisect(trigger_list, trigger_obj.priority - 1)
        index_after = bisect.bisect(trigger_list, trigger_obj)
        found = -1
        for i in range(len(trigger_list[index_before:index_after])):
            if trigger_list[i] == trigger_obj:
                found = i
                break
        return found
