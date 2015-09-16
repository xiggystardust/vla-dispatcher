import os
import struct
import logging
import asyncore, socket
import obsdocxml_parser
import ast
import angles
from jdcal import mjd_now

logger = logging.getLogger(__name__)

class McastClient(asyncore.dispatcher):
    """Generic class to receive the multicast XML docs."""

    def __init__(self, group, port, name=""):
        asyncore.dispatcher.__init__(self)
        self.name = name
        self.group = group
        self.port = port
        addrinfo = socket.getaddrinfo(group, None)[0]
        self.create_socket(addrinfo[0], socket.SOCK_DGRAM)
        self.set_reuse_addr()
        self.bind(('',port))
        mreq = socket.inet_pton(addrinfo[0],addrinfo[4][0]) \
                + struct.pack('=I', socket.INADDR_ANY)
        self.socket.setsockopt(socket.IPPROTO_IP, 
                socket.IP_ADD_MEMBERSHIP, mreq)
        self.read = None

    def handle_connect(self):
        logger.debug('connect %s group=%s port=%d' % (self.name, 
            self.group, self.port))

    def handle_close(self):
        logger.debug('close %s group=%s port=%d' % (self.name, 
            self.group, self.port))

    def writeable(self):
        return False

    def handle_read(self):
        self.read = self.recv(100000)
        logger.debug('read ' + self.name + ' ' + self.read)
        try:
            self.parse()
        except Exception as e:
            logger.exception("error handling '%s' message" % self.name)

    def handle_error(self, type, val, trace):
        logger.error('unhandled exception: ' + repr(val))


class ObsdocClient(McastClient):
    """Receives obsdoc XML, which is broadcast when the BDF is available.

    If the controller input is given, the
    controller.add_obsdoc(obsdoc) method will be called for every
    document received. Controller is defined as a class in the main
    controller script, and runs job launching.
    """

    def __init__(self,controller=None):
        McastClient.__init__(self,'239.192.3.2',53001,'obsdoc')
        self.controller = controller

    def parse(self):
        obsdoc = obsdocxml_parser.parseString(self.read)
        logger.info("Read obsdoc for project %s scan %s subscan %s." % (obsdoc.datasetID,str(obsdoc.scanNo),str(obsdoc.subscanNo)))
        if self.controller is not None:
            self.controller.add_obsdoc(obsdoc)


#A dumbed down version of EVLAconfig just for reading obsdoc info
class MCAST_Config(object):
    """
    This class at the moment just returns info from the SDM
    document. It can easily be expanded to include further information
    from e.g. the OBS doc or VCI doc (e.g. info on observation length,
    SPWs, antenna config, and so forth).
    """

    def __init__(self, obsdoc=None):
        self.set_obsdoc(obsdoc)

    def is_complete(self):
        return self.obsdoc is not None

    def set_obsdoc(self,obsdoc):
        self.obsdoc = obsdoc
        if self.obsdoc is None:
            self.intents = {}
        else:
            self.intents = self.parse_intents(obsdoc.intent)        

    # July 2015:
    # Rich will soon switch from datasetID to datasetId. This takes
    # into account both possibilities
    @property
    def projectID(self):
        if self.obsdoc.datasetId is not None:
            return self.obsdoc.datasetId
        else:
            return self.obsdoc.datasetID

    @property
    def telescope(self):
        return "VLA"

    @property
    def scan(self):
        return self.obsdoc.scanNo

    @property
    def subscan(self):
        return self.obsdoc.subscanNo

    @property
    def intentString(self):
        return self.obsdoc.intent



#-------------
    @staticmethod
    def parse_intents(intents):
        d = {}
        for item in intents:
            k, v = item.split("=")
            if v[0] is "'" or v[0] is '"':
                d[k] = ast.literal_eval(v)
                # Or maybe we should just strip quotes?
            else:
                d[k] = v
        return d

    
    def get_intent(self,key,default=None):
        try:
            return self.intents[key]
        except KeyError:
            return default

    @property
    def Id(self):
        return self.obsdoc.configId

    @property
    def datasetId(self):
        return self.obsdoc.datasetId
    @property
    def observer(self):
        return self.get_intent("ObserverName","Unknown")

    @property
    def projid(self):
        return self.get_intent("ProjectID","Unknown")

    @property
    def scan_intent(self):
        return self.get_intent("ScanIntent","None")

    @property
    def source(self):
        return self.obsdoc.name

    @property
    def ra_deg(self):
        return angles.r2d(self.obsdoc.ra)

    @property
    def ra_hrs(self):
        return angles.r2h(self.obsdoc.ra)

    @property
    def ra_str(self):
        return angles.fmt_angle(self.ra_hrs, ":", ":").lstrip('+-')

    @property
    def dec_deg(self):
        return angles.r2d(self.obsdoc.dec)

    @property
    def dec_str(self):
        return angles.fmt_angle(self.dec_deg, ":", ":")

    @property
    def startLST(self):
        return self.obsdoc.startLST * 86400.0

    @property
    def startTime(self):
        try:
            return float(self.obsdoc.startTime)
        except AttributeError:
            return 0.0

    @property
    def wait_time_sec(self):
        if self.startTime==0.0:
            return None
        else:
            return 86400.0*(self.startTime - mjd_now())

    @property
    def seq(self):
        return self.obsdoc.seq

    def get_sslo(self,IFid):
        """Return the SSLO frequency in MHz for the given IFid.  This will
        correspond to the edge of the baseband.  Uses IFid naming convention 
        as in OBS XML."""
        for sslo in self.obsdoc.sslo:
            if sslo.IFid == IFid:
                return sslo.freq # These are in MHz
        return None

    def get_sideband(self,IFid):
        """Return the sideband sense (int; +1 or -1) for the given IFid.
        Uses IFid naming convention as in OBS XML."""
        for sslo in self.obsdoc.sslo:
            if sslo.IFid == IFid:
                return sslo.Sideband # 1 or -1
        return None

    def get_receiver(self,IFid):
        """Return the receiver name for the given IFid.
        Uses IFid naming convention as in OBS XML."""
        for sslo in self.obsdoc.sslo:
            if sslo.IFid == IFid:
                return sslo.Receiver
        return None

    @staticmethod
    def swbbName_to_IFid(swbbName):
        """Converts values found in the VCI baseBand.swbbName property to
        matching values as used in the OBS sslo.IFid property. 
        
        swbbNames are like AC_8BIT, A1C1_3BIT, etc.
        IFids are like AC, AC1, etc."""

        conversions = {
                'A1C1': 'AC1',
                'A2C2': 'AC2',
                'B1D1': 'BD1',
                'B2D2': 'BD2'
                }

        (bbname, bits) = swbbName.split('_')

        if bbname in conversions:
            return conversions[bbname]

        return bbname


# This is how comms would be used in a program.  Note that no controller
# is passed, so the only action taken here is to print log messages when
# each obsdoc document comes in.
if __name__ == '__main__':
    obsdoc_client = ObsdocClient()
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf on control-C
        logger.info('got SIGINT, exiting')
