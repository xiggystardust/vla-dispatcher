#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script for triggering observations of FRBs.

This is the FCN server script that sends out commands.
"""

import os
import sys
import math
import time
import getopt
import signal
import socket
import struct
import thread
import logging
import traceback
try:
	import cStringIO as StringIO
except ImportError:
	import StringIO
from collections import deque
from datetime import datetime, timedelta

__version__ = '0.1'
__revision__ = '$Rev: 479 $'
__date__ = '$LastChangedDate: 2015-08-27 12:45:57 -0600 (Thu, 27 Aug 2015) $'
__all__ = ['SerialNumber', 'sendNotification', 'notificationEventTypes', 
		 '__version__', '__revision__', '__date__', '__all__']


def usage(exitCode=None):
        print """fcn_server.py - VLA/FRB Coordination Network Notification Server

Usage: fcn_server.py [OPTIONS]

Options:
-h, --help             Display this help information
-f, --hosts-file       Hosts configuration file (Default = hosts.cfg)
-c, --command-file     Incoming command file (Default = incoming.cmd)
-d, --debug            Run in debugging mode (Default = no)
"""
	
	if exitCode is not None:
		sys.exit(exitCode)
	else:
		return True


def parseConfig(args):
	config = {}
	# Command line flags - default values
	config['hosts'] = 'hosts.cfg'
	config['commands'] = 'incoming.cmd'
	config['debug'] = False
	
	# Read in and process the command line flags
	try:
		opts, arg = getopt.getopt(args, "hf:c:d", ["help", "hosts-file=", "command-file=", "debug"])
	except getopt.GetoptError, err:
		# Print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		usage(exitCode=2)
		
	# Work through opts
	for opt, value in opts:
		if opt in ('-h', '--help'):
			usage(exitCode=0)
		elif opt in ('-f', '--hosts-file'):
			config['hosts'] = value
		elif opt in ('-c', '--command-file'):
			config['commands'] = value
		elif opt in ('-d', '--debug'):
			config['debug'] = True
		else:
			assert False
			
	# Return configuration
	return config


def getTime():
	"""
	Return a two-element tuple of the current MJD and MPM.
	"""
	
	# determine current time
	dt = datetime.utcnow()
	year        = dt.year             
	month       = dt.month      
	day         = dt.day    
	hour        = dt.hour
	minute      = dt.minute
	second      = dt.second     
	millisecond = dt.microsecond / 1000
	
	# compute MJD         
	# adapted from http://paste.lisp.org/display/73536
	# can check result using http://www.csgnetwork.com/julianmodifdateconv.html
	a = (14 - month) // 12
	y = year + 4800 - a          
	m = month + (12 * a) - 3                    
	p = day + (((153 * m) + 2) // 5) + (365 * y)   
	q = (y // 4) - (y // 100) + (y // 400) - 32045
	mjd = int(math.floor( (p+q) - 2400000.5))  
	
	# compute MPM
	mpm = int(math.floor( (hour*3600 + minute*60 + second)*1000 + millisecond ))
	
	return (mjd, mpm)


def getGCNTime(timestamp=None):
	"""
	Convert a UNIX timestamp into a TJD/SoD pair, where the seconds of day 
	are in centi-seconds.
	"""
	
	# Is there a time?
	if timestamp is None:
		timestamp = time.time()
		
	# Convert to Julian Day
	jd = float(timestamp) / 86400.0 + 2440587.5
	
	# Calculate TJD
	tjd = int(math.floor(jd - 2440000.5))
	
	# Calculate the second in the day and convert to centiseconds
	sod = (timestamp % 86400)
	sod = int(sod*100)
	
	return tjd, sod


class SerialNumber(object):
	"""
	Simple class for a file-backed serial number generator.
	"""
	
	def __init__(self):
		self.filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.sn')
		
		self.sn = self._getFromFile()
			
	def _getFromFile(self):
		try:
			fh = open(self.filename, 'r')
			sn = int(fh.read(), 10)
			fh.close()
		except IOError:
			sn = 1
			
		return sn
		
	def _putToFile(self, sn):
		status = False
		try:
			fh = open(self.filename, 'w')
			fh.write( str(sn) )
			fh.close()
			
			status = True
		except IOError:
			pass
			
		return status
		
	def get(self):
		out = self.sn
		self.sn = self.sn + 1
		self._putToFile(self.sn)
		
		return out
		
	def reset(self):
		oldSN = self.sn*1
		
		self.sn = 1
		status = self._putToFile(self.sn)
		if not status:
			self.sn = oldSN
			return False
		else:
			return True


# Dictionary that maps event types into their numerical counterparts
notificationEventTypes = {'TEST': 2, 
					 'IAMALIVE': 3, 
					 'KILL': 4, 
					 'VLA_FRB_SESSION': 11, 
					 'VLA_FRB_TRIGGER': 12, }


def _buildPacket(packetType, packetSN, eventSN, eventTime, eventRA, eventDec, eventDuration=None, eventDM=None):
	"""
	Build a binary packed representation of a VLA/FRB program event.  The
	packet formats are based off those used by the GCN and are:
	
	Type:   2            3            4             11                  12
	Loc:    TEST         IMALIVE      KILL          VLA_FRB_SESSION     VLA_FRB_TRIGGER
	
	0      pkt_type     pkt_type     pkt_type       pkt_type            pkt_type
	1      pkt_sernum   pkt_sernum   pkt_sernum     pkt_sernum          pkt_sernum  
	2      pkt_hopcnt   pkt_hopcnt   pkt_hopcnt     pkt_hopcnt          pkt_hopcnt
	3      pkt_sod      pkt_sod      pkt_sod        pkt_sod             pkt_sod
	4      trig_num     -            -              session_num         trig_num
	5      burst_tjd    burst_tjd    burst_tjd      session_tjd         burst_tjd
	6      burst_sod    burst_sod    burst_sod      session_sod         burst_sod
	7      burst_ra     -            -              session_ra          burst_ra
	8      burst_dec    -            -              session_dec         burst_dec
	9      spare        spare        -              session_dur         burst_dm
	10     spare        spare        -              spare               spare
	11     spare        spare        -              spare               spare
	12     spare        spare        -              spare               spare
	13     spare        spare        -              spare               spare
	14     spare        spare        -              spare               spare
	15     spare        spare        -              spare               spare
	16     spare        spare        -              spare               spare
	17     spare        spare        -              spare               spare
	18     spare        spare        -              spare               spare
	19     spare        spare        -              spare               spare
	20     spare        spare        -              spare               spare
	21     spare        spare        -              spare               spare
	22     spare        spare        -              spare               spare
	23     spare        spare        -              spare               spare
	24     spare        spare        -              spare               spare
	25     spare        spare        -              spare               spare
	26     spare        spare        -              spare               spare
	27     spare        spare        -              spare               spare
	28     spare        spare        -              spare               spare
	29     spare        spare        -              spare               spare
	30     spare        spare        -              spare               spare
	31     spare        spare        -              spare               spare
	32     spare        spare        -              spare               spare
	33     spare        spare        -              spare               spare
	34     spare        spare        -              spare               spare
	35     spare        spare        -              spare               spare
	36     spare        spare        -              spare               spare
	37     spare        spare        -              spare               spare
	38     spare        spare        -              spare               spare
	39     pkt_term     pkt_term     pkt_term       pkt_term            pkt_term
	"""
	
	# Packet time marker
	tPacket = time.time()
	
	# Numeric type
	try:
		packetType = notificationEventTypes[packetType]
	except KeyError:
		raise ValueError("Unknown event type '%s'" % packetType)
		
	# Serial number
	packetSN = int(packetSN)
	
	# Hop count
	packetHops = 1
	
	# Packet seconds of day
	packetTJD, packetSoD = getGCNTime()
	
	# Event information
	eventSN = int(eventSN)
	eventTJD, eventSoD = getGCNTime(eventTime)
	eventRA = int(eventRA * 10000)
	eventDec = int(eventDec * 10000)
	if eventDuration is not None:
		eventDuration = int(eventDuration)
		
	# Pack and return
	data = [packetType, packetSN, packetHops, packetSoD, eventSN, eventTJD, eventSoD, eventRA, eventDec]
	if eventDuration is not None:
		data.append( eventDuration )
	if eventDM is not None:
		data.append( eventDM )
	while len(data) < 39:
		data.append( 0 )
	data.append( 10 )
	
	packet = struct.pack('>40l',*data)
	return packet


_socketState = {}
def sendNotification(dests, eventType, eventSN, eventTime, eventRA, eventDec, eventDuration=None, eventDM=None, snGenerator=SerialNumber()):
	"""
	Send an event to one or more clients via TCP.
	"""
	
	# Setup logging
	logger = logging.getLogger(__name__)
	
	# What are we dealing with here?
	try:
		len(dests)
	except TypeError:
		dests = [dests,]
		
	# Generate the serial number
	packetSN = snGenerator.get()
	
	# Loop through the clients
	hostsReached = 0
	for ip,port in dests:
		## Get the port
		try:
			### Already open?
			socketOut = _socketState[(ip,port)]
			
		except KeyError:
			### Try to open the port
			try:
				socketOut = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				socketOut.settimeout(5)
				socketOut.connect((ip, port))
				
				_socketState[(ip,port)] = socketOut
				
			except socket.error as err:
				logger.warning('Cannot connect to %s @ %i: %s', ip, port, str(err))
				continue
				
		## Build the binary packet
		try:
			data = _buildPacket(eventType, packetSN, eventSN, eventTime, eventRA, eventDec, eventDuration=eventDuration, eventDM=eventDM)
		except ValueError:
			continue
			
		## Send and receive
		try:
			socketOut.send(data)
			if eventType != 'KILL':
				reply = socketOut.recv(len(data))
				
			hostsReached += 1
			
		except socket.error as err:
			logger.warning('Cannot send to %s @ %i: %s', ip, port, str(err))
			
			socketOut.close()
			del _socketState[(ip,port)]
			
		## Cleanup for 'Kill'
		if eventType == 'KILL':
			socketOut.close()
			del _socketState[(ip,port)]
			
	# Reset the serial number if the command is 'Kill'
	if eventType == 'KILL':
		snGenerator.reset()
		
	return hostsReached


def main(args):
	"""
	Main function of fcn_server.py.  This sets up the various configuration options 
	and start the UDP command handler.
	"""
	
	# Parse the command line
	config = parseConfig(args)
	
	# Setup logging
	logger = logging.getLogger(__name__)
	logFormat = logging.Formatter('%(asctime)s [%(levelname)-8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
	logFormat.converter = time.gmtime
	logHandler = logging.StreamHandler(sys.stdout)
	logHandler.setFormatter(logFormat)
	logger.addHandler(logHandler)
	if config['debug']:
		logger.setLevel(logging.DEBUG)
	else:
		logger.setLevel(logging.INFO)
		
	# Get current MJD and MPM
	mjd, mpm = getTime()
	
	# Report on who we are
	shortRevision = __revision__.split()[1]
	shortDate = ' '.join(__date__.split()[1:4])
	
	logger.info('Starting fcn_server.py with PID %i', os.getpid())
	logger.info('VLA/FRB Coordination Network Notification Server')
	logger.info('Version: %s', __version__)
	logger.info('Revision: %s', shortRevision)
	logger.info('Last Changed: %s',shortDate)
	logger.info('Current MJD: %i', mjd)
	logger.info('Current MPM: %i', mpm)
	logger.info('All dates and times are in UTC except where noted')
	
	# Setup the list of hosts to send notifications to
	if not os.path.exists(config['hosts']):
		logger.critical('Cannot find the \'%s\' file', os.path.basename(config['hosts']))
		sys.exit()
		
	hosts = []
	fh = open(config['hosts'], 'r')
	for line in fh:
		if line[0] == '#':
			continue
		if len(line) < 3:
			continue
		try:
			ip, port = line.split(None, 1)
			port = int(port, 10)
			hosts.append( (ip,port) )
		except Exception as e:
			logger.warning('WARNING: Cannot parse line \'%s\': %s', line.rstrip(), str(e))
	fh.close()
	logger.info('Loaded %i hosts from \'%s\'', len(hosts), os.path.basename(config['hosts']))
	
	# Report on the incoming command filename
	logger.info('Using \'%s\' for incoming commands', os.path.basename(config['commands']))
	
	# Setup the packet serial number generator
	snGenerator = SerialNumber()
	
	# Loop and process the MCS data packets as they come in - exit if ctrl-c is 
	# received
	logger.info('Ready to communicate')
	t0 = time.time() - 120.0
	try:
		while True:
			try:
				## Is there a command file to read from?
				try:
					fh = open(config['commands'], 'r')
					data = fh.read()
					fh.close()
					
					os.unlink(config['commands'])
				except IOError:
					data = None
					pass
					
				## Is there a command to send?
				if data is not None:
					### Parse out the various fields
					fields = data.split()
					eventType = fields[0].upper()
					eventSN = int(fields[1], 10)
					eventTime = float(fields[2])
					eventRA = float(fields[3])
					eventDec = float(fields[4])
					if eventType.upper() == 'VLA_FRB_SESSION':
						eventDuration = float(fields[5])
					else:
						eventDuration = None
					if eventType.upper() == 'VLA_FRB_TRIGGER':
						eventDM = float(fields[5])
					else:
						eventDM = None
					logger.info("Found new %s command in \'%s\'", eventType, os.path.basename(config['commands']))
					
					### Send it along
					hostsReached = sendNotification(hosts, eventType, eventSN, eventTime, eventRA, eventDec, eventDuration=eventDuration, eventDM=eventDM, snGenerator=snGenerator)
					logger.debug("Sent '%s' to %i hosts", eventType, hostsReached)
					
				## Is it time to send an 'IAMALIVE' packet?
				t1 = time.time()
				if t1-t0 >= 60.0:
					t0 = t1
					
					hostsReached = sendNotification(hosts, 'IAMALIVE', 0, time.time(), 0, 0, snGenerator=snGenerator)
					logger.debug("Sent 'Iamalive' to %i hosts", hostsReached)
					
				time.sleep(0.1)
				
			except Exception, e:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				logger.error("fcn_server.py failed with: %s at line %i", str(e), traceback.tb_lineno(exc_traceback))
					
				## Grab the full traceback and save it to a string via StringIO
				fileObject = StringIO.StringIO()
				traceback.print_tb(exc_traceback, file=fileObject)
				tbString = fileObject.getvalue()
				fileObject.close()
				## Print the traceback to the logger as a series of DEBUG messages
				for line in tbString.split('\n'):
					logger.debug("%s", line)
					
	except KeyboardInterrupt:
		logger.info('Exiting on ctrl-c')
		
		hostsReached = sendNotification(hosts, 'KILL', 0, time.time(), 0, 0, snGenerator=snGenerator)
		
	# If we've made it this far, we have finished so shutdown DP and close the 
	# communications channels
	tStop = time.time()
	print '\nShutting down fcn_server, please wait...'
	logger.info('Shutting down fcn_server, please wait...')
	logger.info('Shutdown completed in %.3f seconds', time.time() - tStop)
	
	# Exit
	logger.info('Finished')
	logging.shutdown()
	sys.exit(0)


if __name__ == "__main__":
	main(sys.argv[1:])
	
