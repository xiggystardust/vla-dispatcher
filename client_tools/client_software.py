#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

This is an example client-end script for listening to vla-slave messages for the LWA.

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



__version__ = '0.0'
__revision__ = '$Rev: 0 $'
__date__ = '$LastChangedDate: Thu, 09 Sep 2015 $'
__all__ = ['Communicate', '__version__', '__revision__', '__date__', '__all__']


# Maximum number of bytes to receive from FRB and MCS
FRB_RCV_BYTES = 4*40
MCS_RCV_BYTES = 16*1024


# Kill packet indicator
FIRST_BYTE_KILL = struct.pack('>l', 4)


def usage(exitCode=None):
        print """receiver_test.py - Test client for VLA/FRB Coordination Network

Usage: receiver_test.py [OPTIONS] HOSTIP PORT

Options:
-h, --help             Display this help information
-d, --debug            Run in debugging mode (Default = no)
"""
	
	if exitCode is not None:
		sys.exit(exitCode)
	else:
		return True


def parseConfig(args):
	config = {}
	# Command line flags - default values
	config['debug'] = False
	config['args'] = []
	
	# Read in and process the command line flags
	try:
		opts, arg = getopt.getopt(args, "hd", ["help", "debug"])
	except getopt.GetoptError, err:
		# Print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		usage(exitCode=2)
		
	# Work through opts
	for opt, value in opts:
		if opt in ('-h', '--help'):
			usage(exitCode=0)
		elif opt in ('-d', '--debug'):
			config['debug'] = True
		else:
			assert False
			
	# Add in the arguments
	config['args'] = arg
	
	# Validate
	if len(args) != 2:
		raise RuntimeError("Must provide a valid IP address and port for this host")
		
	# Return configuration
	return config


def getTime():
	"""
	Return a two-element tuple of the current MJD and MPM (floor of milliseconds since midnight).
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


# A few time conversion tools...
"""                                                                                                        
Offset in days between UNIX time (epoch 1970/01/01) and standard Julian day.                               
"""
UNIX_OFFSET = 2440587.5
"""                                                                                                        
The number of seconds in one day                                                                           
"""
SECS_IN_DAY = 86400.0
"""                                                                                                        
Offset in days between standary Julian day and modified Julian day.                                        
"""
MJD_OFFSET = 2400000.5
def utcjd_to_unix(utcJD):
	"""                                                                                                
        Get UNIX time value for a given UTC JD value.                                                      
                                                                                                           
        Param: utcJD - The UTC JD time (float).                                                            
                                                                                                           
        Returns: The UNIX time                                                                             
        """

	unixTime = (utcJD - UNIX_OFFSET) * SECS_IN_DAY
        return unixTime



class Communicate(object):
	"""
	Class to deal with the communicating via TCP with the VLA/FRB program and UDP with HAL.
	"""
	
	def __init__(self, config):
		self.config = config
		
		# Update the socket configuration
		self.updateConfig()
		
		# Setup the packet queues using deques
		self.queueIn  = deque()
		
		# Set the logger
		self.logger = logging.getLogger('__main__')
		
		# Setup an attribute to keep track of the last packet
		self.lastPacket = None
		
	def updateConfig(self, config=None):
		"""
		Using the configuration file, update the list of boards.
		"""
		
		# Update the current configuration
		if config is not None:
			self.config = config
			
	def start(self):
		"""
		Start the receive thread - send will run only when needed.
		"""
		
		# Clear the packet queue
		self.queueIn  = deque()
		
		# Start the packet processing thread
		thread.start_new_thread(self.packetProcessor, ())
		
		# Setup the various sockets
		## Receive
		try:
			self.socketIn =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.socketIn.bind((self.config['MESSAGEINHOST'], self.config['MESSAGEINPORT']))
			self.socketIn.listen(1)
			self.client = None
			
			self.lastPacket = time.time()
			
		except socket.error, err:
			code, e = err
			self.logger.critical('Cannot bind to listening port %i: %s', self.config['MESSAGEINPORT'], str(e))
			self.logger.critical('Exiting on previous error')
			logging.shutdown()
			sys.exit(1)
			
	def stop(self):
		"""
		Stop the receive thread, waiting until it's finished.
		"""
		
		# Clear the packet queue
		self.queueIn.append(('STOP_THREAD', '0.0.0.0'))
		
		# Close the various sockets
		if self.client is not None:
			self.client.close()
		self.socketIn.close()
		
	def receiveNotification(self):
		"""
		Receive and process a VLA/FRB packet over the network and add it to 
		the packet processing queue.
		"""
		
		if self.client is None:
			self.client, self.address = self.socketIn.accept()
			self.client.settimeout(1800)
			
		try:
			data = self.client.recv(FRB_RCV_BYTES)
		except socket.timeout:
			data = ''
			
		if data:
			if data[:4] != FIRST_BYTE_KILL:
				self.client.sendall(data)
				
			self.queueIn.append((data,self.address[0]))
			
			fh = open('log', 'ab')
			fh.write(data)
			fh.close()
			
			self.lastPacket = time.time()
		else:
			if time.time() - self.lastPacket > 3600.0:
				self.logger.debug("No packets received in the last hour, shutting down connection")
				self.client.close()
				self.client = None
			
	def packetProcessor(self):
		"""
		Using two deques (one inbound, one outbound), deal with bursty UDP 
		traffic by having a separate thread for processing commands.
		"""
		
		exitCondition = False
		
		while True:
			while len(self.queueIn) > 0:
				try:
					data,dest = self.queueIn.popleft()
					if data is 'STOP_THREAD':
						exitCondition = True
						break
						
					# Below is a four-element tuple of:
					# * destination
					# * command name (i.e. "TRF")
					# * command arguments (i.e. RA, Dec, etc.)
					# * reference number
					destination, command, packed_data, reference = self.processNotification(data)

					#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
					#!!! Here's where to trigger decision-making for whether to observe or not. !!!
					#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
					#
					# The event information, packed_data, can also be packed into a struct (see processNotification).
				        # It can be unpacked into a tuple with e.g. unpacked_data = struct.unpack('>64sffd', packed_data).
					#
					# However, this client program passes information to here as a list. The list is packed as
					# eventName, eventRA, eventDec, eventTimestamp, eventAdd(=event duration=eventDur).
					# 
					# Event duration (eventDur) passed from the VLA will be NEGATIVE OR ZERO to signify the end of the
					# VLA's observation. It will be POSITIVE with units of seconds to signify the "timeout" of an
					# observation---that is, the receiving telescope should aim to observe for eventDur seconds unless
					# it receives an event with eventDur<=0. This "end event" message will have the same event
					# name+number as the "start event" message for that VLA pointing.
					# 
					
					# If we've received a VLA event packet...
					if (packed_data is not None):

						# Unpack the event information
						eventName, eventRA, eventDec, eventTime, eventDur = packed_data

						# Is this a "VLA observation" event or some other event?
						# (note, only obs events are supported on VLA-end right now)
						if ("VLA_FRB_SESSION" in eventName):

							# Start observing or end observing?
							if eventDur > 0:
								# DECIDE WHETHER TO START OBSERVATION.
								self.logger.info("Found START notice for session %s" % eventName)
								self.logger.debug("Session info: " % ' '.join(str(val) for val in packed_data))
								self.logger.info("I will now observe RA/Dec %d %d for %d seconds." % (eventRA,eventDec,eventDur))
							else:
								# DECIDE WHETHER TO STOP OBSERVATION.
								self.logger.info("Found END notice for session %s" % eventName)
								self.logger.debug("Session info: " % ' '.join(str(val) for val in packed_data))
								self.logger.info("I will now CEASE observation of (%d, %d)." % (eventRA,eventDec))

					#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!H
					#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!



				except Exception, e:
					exc_type, exc_value, exc_traceback = sys.exc_info()
					self.logger.error("packetProcessor failed with: %s at line %i", str(e), traceback.tb_lineno(exc_traceback))
						
					## Grab the full traceback and save it to a string via StringIO
					fileObject = StringIO.StringIO()
					traceback.print_tb(exc_traceback, file=fileObject)
					tbString = fileObject.getvalue()
					fileObject.close()
					## Print the traceback to the logger as a series of DEBUG messages
					for line in tbString.split('\n'):
						self.logger.debug("%s", line)
						
			if exitCondition:
				break
				
			time.sleep(0.010)
			
	def parsePacket(self, rawData):
		"""
		Given a VLA/FRB TCP notification packet, break it into its various 
		parts and return them as an seven-element tuple.  The parts are:
		  1. type
		  2. serial number
		  3. ID
		  4. event UNIX timestamp, seconds
		  5. right assension, degrees
		  6. declination, degrees
		  7. additional information (duration in s for a VLA_FRB_SESSION;
		     DM for a VLA_FRB_TRIGGER)
		"""
		
		# Unpack 40 4-byte intgers
		data = struct.unpack('>40l', rawData)
		
		# Figure out the notification type
		notifyType = data[0]
		if notifyType == 2:
			notifyType = "Test"
		elif notifyType == 3:
			notifyType = "Iamalive"
		elif notifyType == 4:
			notifyType = "Kill"
		elif notifyType == 11:
			notifyType = "VLA_FRB_SESSION"
		elif notifyType == 12:
			notifyType = "VLA_FRB_TRIGGER"
		else:
			notifyType = "Unknown"
			
		# Serial number
		sn = data[1]
		
		# Event name
		eventName = "%s #%i" % (notifyType, data[4])
		
		# Event timestamp
		jd = data[5] + 40000 + data[6]/100.0/SECS_IN_DAY + MJD_OFFSET
		eventTimestamp = utcjd_to_unix(jd)
		
		# Event position and uncertainty
		eventRA = data[7] / 10000.0
		eventDec = data[8] / 10000.0
		eventAdd = data[9]
		
		# Validation
		if data[39] != 10:
			self.logger.warning("Packet '%s' with size %i B may be invalid because of an invalid terminator", rawData, len(rawData))
			
		return notifyType, sn, eventName, eventTimestamp, eventRA, eventDec, eventAdd
		
	def processNotification(self, data):
		"""
		Interpret the data of a TCP "packet" as a FRB trigger.
		
		Returns a four-elements tuple of:
		  * destination
		  * command name
		  * command arguments
		  * reference number
		"""
		
		notifyType, sn, eventName, eventTimestamp, eventRA, eventDec, eventAdd = self.parsePacket(data)
		
		self.logger.debug("Checking packet type %s" % notifyType)
		if notifyType in ('Unknown', 'Invalid'):
			self.logger.error("Unknown FRB notification type '%s' (serial# %i), dropping", notifyType,sn)
			return None, None, None, None
			
		elif notifyType in ('Iamalive',):
			self.logger.debug("'Iamalive' packet received (serial# %i), dropping", sn)
			return None, None, None, None
			
		elif notifyType in ('Kill',):
			self.logger.debug("Remote kill request (serial# %i). Shutting down connection.", sn)
			self.client.close()
			self.client = None
			
			return None, None, None, None
			
		elif notifyType in ('VLA_FRB_TEST',):
			self.logger.debug("'VLA_FRB_TEST' packet received with S/N %i, dropping", sn)
			return None, None, None, None
			
		else:
			self.logger.debug('Got event %s from VLA/FRB Program: S/N #%i', eventName, sn)
			# Return status, command, reference, and the result
			destination = 'HAL'
			command = 'TRF'
			#packed_data = struct.pack('>64sffd', eventName, eventRA, eventDec, eventTimestamp)
			packed_data = [eventName, eventRA, eventDec, eventTimestamp, eventAdd]
			reference = sn
			
			return destination, command, packed_data, reference


def main(args):
	"""
	Main function of receiver_test.py.  This sets up the various configuration options 
	and start the UDP command handler.
	"""
	
	# Parse the command line
	config = parseConfig(args)
	ip, port = config['args']
	port = int(port, 10)
	
	# Setup logging
	logger = logging.getLogger(__name__)
	logFormat = logging.Formatter('%(asctime)s [%(levelname)-8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
	logFormat.converter = time.gmtime
	logHandler = logging.StreamHandler(sys.stdout)
	logHandler.setFormatter(logFormat)
	logger.addHandler(logHandler)
	logger.setLevel(logging.DEBUG)
	
	# Get current MJD and MPM
	mjd, mpm = getTime()
	
	# Report on who we are
	shortRevision = __revision__.split()[1]
	shortDate = ' '.join(__date__.split()[1:4])
	
	logger.info('Starting receiver_test.py with PID %i', os.getpid())
	logger.info('Receiver - FRB Announcement Capture - Test Client')
	logger.info('Version: %s', __version__)
	logger.info('Revision: %s', shortRevision)
	logger.info('Last Changed: %s',shortDate)
	logger.info('Current MJD: %i', mjd)
	logger.info('Current MPM: %i', mpm)
	logger.info('All dates and times are in UTC except where noted')
	
	# Setup the configuration
	config = {'MESSAGEINHOST': ip,
			'MESSAGEINPORT': port,}
			
	# Setup the communications channels
	frbComms = Communicate(config)
	frbComms.start()
	
	# Setup handler for SIGTERM so that we aren't left in a funny state
	def HandleSignalExit(signum, frame, logger=logger, CommInstance=frbComms):
		logger.info('Exiting on signal %i', signum)

		# Shutdown receiver_test and close the communications channels
		tStop = time.time()
		logger.info('Shutting down receiver_test.py, please wait...')
		
		logger.info('Shutdown completed in %.3f seconds', time.time() - tStop)
		
		CommInstance.stop()
		
		# Exit
		logger.info('Finished')
		logging.shutdown()
		sys.exit(0)
	
	# Hook in the signal handler - SIGTERM
	signal.signal(signal.SIGTERM, HandleSignalExit)
	
	# Loop and process the MCS data packets as they come in - exit if ctrl-c is 
	# received
	logger.info('Receiving line open.')
	while True:
		try:
			frbComms.receiveNotification()
			
		except KeyboardInterrupt:
			logger.info('Exiting on ctrl-c')
			break
			
		except Exception, e:
			exc_type, exc_value, exc_traceback = sys.exc_info()
			logger.error("receiver_test.py failed with: %s at line %i", str(e), traceback.tb_lineno(exc_traceback))
				
			## Grab the full traceback and save it to a string via StringIO
			fileObject = StringIO.StringIO()
			traceback.print_tb(exc_traceback, file=fileObject)
			tbString = fileObject.getvalue()
			fileObject.close()
			## Print the traceback to the logger as a series of DEBUG messages
			for line in tbString.split('\n'):
				logger.debug("%s", line)
	
	# If we've made it this far, we have finished so shutdown DP and close the 
	# communications channels
	tStop = time.time()
	print '\nShutting down receiver_test, please wait...'
	logger.info('Shutting down receiver_test, please wait...')
	logger.info('Shutdown completed in %.3f seconds', time.time() - tStop)
	frbComms.stop()
	
	# Exit
	logger.info('Finished')
	logging.shutdown()
	sys.exit(0)


if __name__ == "__main__":
	main(sys.argv[1:])
	
