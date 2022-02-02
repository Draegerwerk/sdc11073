import os
import time
from threading import Lock

D_IN = 'in'
D_OUT = 'out'

T_UDP = 'udp'
T_UDPBroadcast = 'udpB'
T_HTTP = 'http'
T_WSDL = 'wsdl'

T_HTTP_REQ = 'http_req'
T_HTTP_RESP = 'http_resp'


class NullLogger(object):
    """This is a dummy logger that does nothing."""
    def __getattr__(self, name):
        return self.do_nothing
    
    def do_nothing(self, *args, **kwargs):
        pass


class CommLogger(object):
    """This is the logger that writes communication logs."""
    def __init__(self, log_folder, log_out=False, log_in=False, broadcastIpFilter=None):
        self._log_folder = log_folder
        self._log_out = log_out
        self._log_in = log_in
        self._broadcastIpFilter = broadcastIpFilter
        self._counter = 1
        self._ioLock = Lock()

        self._mkLogFolder(log_folder)


    def setBroadcastIpFilter(self, broadcastIpFilter):
        self._broadcastIpFilter = broadcastIpFilter

    @staticmethod        
    def _mkLogFolder(path):
        if not os.path.exists(path):
            os.makedirs(path) 


    def _mkFileName(self, ipType, direction, info):
        '''
        @param ipType: "tcp" or "udp"
        @param direction: "in" or "out"
        '''
        assert ipType in (T_UDP, T_UDPBroadcast, T_HTTP, T_HTTP_REQ, T_HTTP_RESP, T_WSDL)
        assert direction in (D_IN, D_OUT)
        extension = 'wsdl' if ipType == T_WSDL else 'xml'
        timestring = '{:06.3f}'.format(time.time())[-8:]
        self._counter += 1
        infotext = '-{}'.format(info) if info else ''
        return '{}-{}-{}{}.{}'.format(timestring, direction, ipType, infotext, extension)

    def _writeLog(self, ttype, direction, xml, info):
        path = os.path.join(self._log_folder, self._mkFileName(ttype, direction, info))
        with self._ioLock:
            with open (path,'wb') as f:
                f.write(xml)
    
    def logBroadCastMsgOut(self, xml, info=None):
        if self._log_out:
            self._writeLog(T_UDPBroadcast, D_OUT, xml, info)
            
    def logDiscoveryMsgOut(self, ipaddr, xml, info=None):
        if self._log_out and (not self._broadcastIpFilter or self._broadcastIpFilter == ipaddr):
            self._writeLog(T_UDP, D_OUT, xml, info)
    
    def logDiscoveryMsgIn(self, ipaddr, xml):
        if self._log_in and (not self._broadcastIpFilter or self._broadcastIpFilter == ipaddr):
            self._writeLog(T_UDP, D_IN, xml, None)

    def logSoapReqIn(self, xml, info=None):
        if self._log_in:
            self._writeLog(T_HTTP_REQ, D_IN, xml, info)

    def logSoapReqOut(self, xml, info=None):
        if self._log_out:
            self._writeLog(T_HTTP_REQ, D_OUT, xml, info)

    def logSoapRespIn(self, xml, info=None):
        if self._log_in:
            self._writeLog(T_HTTP_RESP, D_IN, xml, info)

    def logSoapRespOut(self, xml, info=None):
        if self._log_out:
            self._writeLog(T_HTTP_RESP, D_OUT, xml, info)

    def logSoapSubscrMsgIn(self, xml):
        if self._log_in:
            self._writeLog(T_HTTP, D_IN, xml, 'subscr')
        
    def logWsdl(self, wsdl ):
        if self._log_in:
            self._writeLog(T_WSDL, D_IN, wsdl, None)

defaultLogger = NullLogger()

def getCommunicationLogger():
    return defaultLogger

def setCommunicationLogger(comm_logger):
    global defaultLogger
    defaultLogger = comm_logger