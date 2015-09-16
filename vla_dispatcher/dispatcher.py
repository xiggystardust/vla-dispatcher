#! /usr/bin/env python2.7
#
# VLA DISPATCHER.
#
# Reads MCAF stream from VLA and sends position and timing commands to
# experiments who wish to coordinate observing with the VLA.
#
# Currently reads all info from the multicast OBSDOC
#
# Sarah Burke Spolaor Sep 2015
#
#
"""
Still to do;
 1. Hook up to LWA comms software.
 2. Deal with scan duration (input line?).
 3. Correct triggering; only trigger at first scan of SB?
"""


import datetime
import os
import asyncore
import logging
from optparse import OptionParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import mcaf_library

# set up
telcaldir = '/home/mchammer/evladata/telcal'  # then yyyy/mm
workdir = os.getcwd()     # assuming we start in workdir
redishost = os.uname()[1]  # assuming we start on redis host

class FRBController(object):
    """Listens for OBS packets and tells FRB processing about any
    notable scans."""

    def __init__(self, intent='', project='', dispatch=False, verbose=False):
        # Mode can be project, intent
        self.intent = intent
        self.project = project
        self.dispatch = dispatch
        self.verbose = verbose

    def add_obsdoc(self, obsdoc):
        config = mcaf_library.MCAST_Config(obsdoc=obsdoc)

        # If intent and project are good, print stuff.
        if (self.intent in config.scan_intent or self.intent is "") and (self.project in config.projectID or self.project is ""):
            logger.info("*** Scan %d contains desired intent (%s=%s) and project (%s=%s)." % (config.scan, config.scan_intent,self.intent, config.projectID,self.project))
            logger.info("*** Position is (%s , %s) and start time (%s; LST %s).\n" % (config.ra_str,config.dec_str,str(config.startTime),str(config.startLST)))

            # If we're not in listening mode, take action
            if self.dispatch:
                logger.info("We're in DISPATCH mode! Will Dispatch commands.")

        else:
            logger.info("*** Skipping scan %d (%s, %s)." % (config.scan, config.scan_intent,config.projectID))
            #logger.info("*** Position is (%s , %s) and start time (%s; LST %s).\n" % (config.ra_str,config.dec_str,str(config.startTime),str(config.startLST)))


def monitor(intent, project, dispatch, verbose):
    """ Monitor of mcaf observation files. 
    Scans that match intent and project are searched (unless --dispatch).
    Blocking function.
    """

    # Set up verbosity level for log
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Report start-up information
    logger.info('* * * * * * * * * * * * * * * * * * * * *')
    logger.info('* * * VLA Dispatcher is now running * * *')
    logger.info('* * * * * * * * * * * * * * * * * * * * *')
    logger.info('*   Looking for intent = %s, project = %s' % (intent, project))
    logger.debug('*   Running in verbose mode')
    if dispatch:
        logger.info('*   Running in dispatch mode. Will dispatch obs commands.')
    else:
        logger.info('*   Running in listening mode. Will not dispatch obs commands.')
    logger.info('* * * * * * * * * * * * * * * * * * * * *\n')

    # This starts the receiving/handling loop
    controller = FRBController(intent=intent, project=project, dispatch=dispatch, verbose=verbose)
    obsdoc_client = mcaf_library.ObsdocClient(controller)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf
        logger.info('Escaping mcaf_monitor')



#@click.command()
#@click.option('--intent', '-i', default='', help='Intent to trigger on')
#@click.option('--project', '-p', default='', help='Project name to trigger on')
#@click.option('--dispatch/--do', '-l', help='Only dispatch to multicast or actually do work?', default=True)
#@click.option('--verbose', '-v', help='More verbose output', is_flag=True)
if __name__ == '__main__':
    # This starts the receiving/handling loop

    cmdline = OptionParser()
    cmdline.add_option('-i', '--intent', dest="intent",
        action="store", default="",
        help="[] Trigger on what intent substring?")
    cmdline.add_option('-p', '--project', dest="project",
        action="store", default="",
        help="[] Trigger on what project substring?")
    cmdline.add_option('-d', '--dispatch', dest="dispatch",
        action="store_true", default=False,
        help="[False] Actually run dispatcher; don't just listen to multicast.") 
    cmdline.add_option('-v', '--verbose', dest="verbose",
        action="store_true", default=False,
        help="[False] Verbose output")
    (opt,args) = cmdline.parse_args()

    monitor(opt.intent, opt.project, opt.dispatch, opt.verbose)
