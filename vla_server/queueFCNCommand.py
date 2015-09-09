#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script that get commands ready for fcn_server.py to send them out
"""


import os
import sys
import time
import getopt


def usage(exitCode=None):
        print """queueFCNCommand.py - Get a command ready to go out over the FRB Coordination Network

Usage: queueFCNCommand.py [OPTIONS] EventType EventID EventTime EventRA EventDec [EventDuration] [EventDM]

EventType:  One of TEST, VLA_FRB_SESSION, VLA_FRB_TRIGGER
EventID: ID number for this type of event
EventTime: Event time in seconds since the UNIX Epoch (0 = now)
EventRA: Event RA in degrees, J2000.0
EventDec: Event declination in degrees, J2000.0
EventDuration: Required for VLA_FRB_SESSION, session duration in seconds
EventDM: Required for VLA_FRB_TRIGGER, event DM in pc/cm^2

Options:
-h, --help             Display this help information
-c, --command-file     Incoming command file to write to (Default = 
                       incoming.cmd)
"""
	
	if exitCode is not None:
		sys.exit(exitCode)
	else:
		return True


def parseConfig(args):
	config = {}
	# Command line flags - default values
	config['commands'] = 'incoming.cmd'
	config['args'] = []
	
	# Read in and process the command line flags
	try:
		opts, arg = getopt.getopt(args, "hc:", ["help", "command-file=",])
	except getopt.GetoptError, err:
		# Print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		usage(exitCode=2)
		
	# Work through opts
	for opt, value in opts:
		if opt in ('-h', '--help'):
			usage(exitCode=0)
		elif opt in ('-c', '--command-file'):
			config['commands'] = value
		else:
			assert False
			
	# Add in the arguments
	config['args'] = args
	
	# Return configuration
	return config


def main(args):
	# Parse the command line
	config = parseConfig(args)
	
	# Make sense of the input
	fields = config['args']
	## Type
	eventType = fields[0]
	if eventType not in ('TEST', 'VLA_FRB_SESSION', 'VLA_FRB_TRIGGER'):
		raise RuntimeErrror("Unsupported FCN event type '%s'", eventType)
	## ID number
	eventSN = int(fields[1], 10)
	## Time
	eventTime = float(fields[2])
	if eventTime == 0.0:
		eventTime = time.time()
	## Location
	eventRA = float(fields[3])
	eventDec = float(fields[4])
	## Extra information
	if eventType == 'VLA_FRB_SESSION':
		eventDur = int(fields[5], 10)
	else:
		eventDur = None
	if eventType == 'VLA_FRB_TRIGGER':
		eventDM = float(fields[5])
	else:
		eventDM = None
		
	# Get ready to send...
	print "Waiting for command queue to clear...",
	while os.path.exists(config['commands']):
		time.sleep(1)
	print "Done"
	
	# Go!
	print "Queueing...", 
	fh = open(config['commands'], 'w')
	fh.write("%s %i %f %f %f" % (eventType, eventSN, eventTime, eventRA, eventDec))
	if eventType == 'VLA_FRB_SESSION':
		fh.write("  %s" % eventDur)
	if eventType == 'VLA_FRB_TRIGGER':
		fh.write("  %s" % eventDM)
	fh.close()
	print "Done, wrote %i B" % os.path.getsize(config['commands'])


if __name__ == "__main__":
	main(sys.argv[1:])
	
