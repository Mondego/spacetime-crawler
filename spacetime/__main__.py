#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import argparse
import json
import sys
from subprocess import Popen
from datamodel.all import DATAMODEL_TYPES
from spacetime.server.start import start_server
from spacetime.server.store import dataframe_stores

# pylint: enable=W0613
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        '-p', '--port', type=int, default=12000,
        help='Port where the server will listen (default: 12000)')
    PARSER.add_argument(
        '-P', '--profile', action='store_true',
        help='Enable profiling on store server.')
    PARSER.add_argument(
        '-d', '--debug', action='store_true', help='Debug on')
    PARSER.add_argument(
        '-i', '--trackip', action='store_true', default=False,
        help='Starts an ip tracker')
    PARSER.add_argument(
        '-t', '--timeout', type=int, default=0,
        help='Timeout in seconds for the server to consider '
             'a client disconnected.')
    PARSER.add_argument(
        '-c', '--clearempty', action='store_true', default=False,
        help='Clears the dataframes when all simulations leave.')
    PARSER.add_argument(
        '-cf', '--config_file', type=str, default=None,
        help='Json file with spacetime configurations stored.')
    PARSER.add_argument(
        '-o', '--object', action='store_true', default=False,
        help='Sets up the server using objectless dataframe. '
             'If False, full dataframe is used.')
    PARSER.add_argument(
        '-ltp', '--load_types', nargs="+", default=list(),
        help='loads the types from the datamodel.all.DATAMODEL_TYPES '
             'that has to be loaded.')
    PARSER.add_argument(
        '-v', '--version', action='store_true', default=False,
        help='Returns the version of spacetime and rtypes used.')


    ARGS = PARSER.parse_args()
    if ARGS.version:
        import spacetime, rtypes
        print "Spacetime Version is ", spacetime.version
        print "Rtypes Version is ", rtypes.version
        sys.exit(0)
    ARGS = PARSER.parse_args()
    CONFIG = json.load(open(ARGS.config_file)) if ARGS.config_file else dict()
    LOAD_TYPES = (
        CONFIG["load_types"] if "load_types" in CONFIG else ARGS.load_types)
    NAME2CLASS = {
        tp.__rtypes_metadata__.name: tp
        for tp in DATAMODEL_TYPES
        if ((tp.__rtypes_metadata__.name in set(LOAD_TYPES))
            if LOAD_TYPES else
            True)}
    STORE = dataframe_stores(NAME2CLASS, dict(), not ARGS.object)
    try:
        SERVER = start_server(
            STORE, args=ARGS, config=CONFIG, console=False)
        SERVER.join()
    except KeyboardInterrupt:
        SERVER.shutdown()
    finally:
        Popen("stty sane", shell=True)
