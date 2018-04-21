#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import argparse
import sys
from datamodel.all import DATAMODEL_TYPES


# pylint: enable=W0613
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        '-v', '--version', action='store_true', default=False,
        help='Returns the version of spacetime and rtypes used.')


    ARGS = PARSER.parse_args()
    if ARGS.version:
        import spacetime, rtypes
        print "Spacetime Version is ", spacetime.version
        print "Rtypes Version is ", rtypes.version
        sys.exit(0)
