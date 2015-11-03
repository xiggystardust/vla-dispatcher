#!/usr/bin/env python2.7
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
from time import gmtime,strftime
from optparse import OptionParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import mcaf_library

# GLOBAL VARIABLES
workdir = os.getcwd() # assuming we start in workdir
dispatched = {};      # Keep global list of dispatched commands
MJD_OFFSET = 2400000.5 # Offset in days between standard Julian day and MJD

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
            if config.source in "FINISH":
                logger.info("*** Project %s has finished (source=%s)" % (config.projectID,config.source))
            else:
                logger.info("*** Scan %d contains desired intent (%s=%s) and project (%s=%s)." % (config.scan, config.scan_intent,self.intent, config.projectID,self.project))
                logger.info("*** Position of source %s is (%s , %s) and start time (%s; unixtime %s)." % (config.source,config.ra_str,config.dec_str,str(config.startTime),str(mcaf_library.utcjd_to_unix(config.startTime+MJD_OFFSET))))

            # If we're not in listening mode, take action
	    if self.dispatch:

                # Check whether obs has completed, obs is continuing, or obs is a new obs.
                do_dispatch = False
                if config.source in 'FINISH':
                    if config.projectID in dispatched:
                        # Set "finish" parameters.
                        eventType = 'VLA_FRB_SESSION'
                        eventTime = mcaf_library.utcjd_to_unix(config.startTime+MJD_OFFSET)
                        eventRA   = config.ra_deg
                        eventDec  = config.dec_deg
                        eventDur  = -1. # To signify "stop obs" command.
                        eventSN = int(dispatched[config.projectID][1])
                        do_dispatch = True
                        
                        # Remove project from dispatched tracker and check for unresolved jobs.
                        del dispatched[config.projectID]
                        if len(dispatched) is not 0:
                            logger.debug("Dispatched jobs remaining:" % '\n'.join(['%s %s' % (key, value) for (key, value) in dispatched.items()]))
                    else:
                        logger.debug("Finished SB is not in dispatched list.")

                elif ('TARGET' not in config.scan_intent):
                    logger.info("This is not a target scan. Will take no action.")
                elif (config.projectID in list(dispatched.keys())) and (config.source in dispatched[config.projectID]):
                    logger.info("Project %s already dispatched for target %s" % (config.projectID, config.source))
                else:
                #!!! CHECK FOR FINAL MESSAGE; SHOULD WE SEND A STOP COMMAND? REMOVE FROM dispatched IF SENT.
                    logger.info("Will dispatch %s for position %s %s" % (config.projectID,config.ra_str,config.dec_str))
                    
                    # Event serial number (eventSN) is UTC YYMMDDHHMM.
                    # This convention will work up to and including the year 2021.
                    eventType = 'VLA_FRB_SESSION'
                    eventTime = mcaf_library.utcjd_to_unix(config.startTime+MJD_OFFSET)
                    eventRA   = config.ra_deg
                    eventDec  = config.dec_deg
                    eventDur  = 10800. # In seconds. 1 hour + 5 minutes auto stop-obs. Positive number signifies "start obs" command"
                    eventSN = int(strftime("%y%m%d%H%M",gmtime()))
                    do_dispatch = True

                    # Add dispatched project and target to dispatched tracker.
                    dispatched[config.projectID] = (config.source,eventSN)

                
                # Is this a command we actually want to dispatch?
                if do_dispatch:
                    # Wait until last command disappears (i.e. cmd file is deleted by server)
                    cmdfile = 'incoming.cmd'
                    if os.path.exists(cmdfile):
                        logger.info("Waiting for cmd queue to clear...")
                    while os.path.exists(cmdfile):
                        time.sleep(1)

                    # Enqueue command
                    if (eventDur>0):
                        logger.info("Dispatching START command for obs serial# %s." % eventSN)
                    else:
                        logger.info("Dispatching STOP command for obs serial# %s." % eventSN)
                    fh = open(cmdfile,'w')
                    fh.write("%s %i %f %f %f %f" % (eventType, eventSN, eventTime, eventRA, eventDec, eventDur))
                    fh.close()
                    logger.info("Done, wrote %i bytes.\n" % os.path.getsize(cmdfile))

                
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
