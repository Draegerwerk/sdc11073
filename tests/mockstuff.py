import threading
import logging
import os.path
from six.moves import urllib
from sdc11073.pysoap.soapenvelope import Soap12Envelope, DPWSThisModel, DPWSThisDevice
from sdc11073.sdcdevice.subscriptionmgr import _DevSubscription
from sdc11073.mdib import DeviceMdibContainer
from sdc11073 import namespaces
from sdc11073 import pmtypes

from sdc11073.sdcdevice import  PublishingSdcDevice
from lxml import etree as etree_
portsLock = threading.Lock()
_ports = 10000

_mockhttpservers = {}

_logger = logging.getLogger('sdc.mock')

def resetModule():
    global _ports
    _mockhttpservers.clear()
    _ports = 10000

def _findServer(netloc):
    dev_addr = netloc.split(':')
    dev_addr = tuple([dev_addr[0], int(dev_addr[1])]) # make port number an integer
    for key, srv in _mockhttpservers.items():
        if tuple(key) == dev_addr:
            return srv
    raise KeyError('{} is not in {}'.format(dev_addr, _mockhttpservers.keys() ))



class MockWsDiscovery(object):
    def __init__(self, ipaddresses):
        self._ipaddresses = ipaddresses
    
    def getActiveAddresses(self):
        return self._ipaddresses

    def clearService(self, epr):
        _logger.info ('clearService "{}"'.format(epr))



class TestDevSubscription(_DevSubscription):
    ''' Can be used instead of real Subscription objects'''
    mode = 'SomeMode'
    notifyTo = 'http://self.com:123'
    identifier = '0815'
    expires = 60
    notifyRef = 'a ref string'
    def __init__(self, filter_, bicepsSchema):
        notifyRefNode = etree_.Element(namespaces.wseTag('References'))
        identNode = etree_.SubElement(notifyRefNode, namespaces.wseTag('Identifier'))
        identNode.text = self.notifyRef
        base_urls = [ urllib.parse.SplitResult('https', 'www.example.com:222', 'no_uuid', query=None, fragment=None)]

        super(TestDevSubscription, self).__init__(mode=self.mode, 
                                                  notifyToAddress=self.notifyTo, 
                                                  notifyRefNode=notifyRefNode,
                                                  endToAddress=None,
                                                  endToRefNode=None,
                                                  expires=self.expires,
                                                  max_subscription_duration=42,
                                                  filter_=filter_,
                                                  sslContext=None,
                                                  bicepsSchema=bicepsSchema,
                                                  acceptedEncodings=None,
                                                  base_urls=base_urls)
        self.reports = []
        self.bmmSchema = bicepsSchema.bmmSchema
        
        
    def sendNotificationReport(self, bodyNode, action, doc_nsmap):
        soapEnvelope = Soap12Envelope(doc_nsmap)
        soapEnvelope.addBodyElement(bodyNode)
        rep = self._mkNotificationReport(soapEnvelope, action)
        try:
            rep.validateBody(self.bmmSchema)
        except:
            print (rep.as_xml(pretty=True))
            raise
        self.reports.append(rep)


class SomeDevice(PublishingSdcDevice):
    """A device used for unit tests

    """
    def __init__(self, wsdiscovery, my_uuid, mdib_xml_string,
                 validate=True, useSSL=True, sslContext=None, logLevel=logging.INFO, log_prefix=''):
        model = DPWSThisModel(manufacturer='Draeger CoC Systems',
                              manufacturerUrl='www.draeger.com',
                              modelName='SomeDevice',
                              modelNumber='1.0',
                              modelUrl='www.draeger.com/whatever/you/want/model',
                              presentationUrl='www.draeger.com/whatever/you/want/presentation')
        device = DPWSThisDevice(friendlyName='Py SomeDevice',
                                firmwareVersion='0.99',
                                serialNumber='12345')
#        log_prefix = '' if not ident else '<{}>:'.format(ident)
        deviceMdibContainer = DeviceMdibContainer.fromString(mdib_xml_string, log_prefix=log_prefix)
        # set Metadata
        mdsDescriptor = deviceMdibContainer.descriptions.NODETYPE.getOne(namespaces.domTag('MdsDescriptor'))
        mdsDescriptor.Manufacturer.append(pmtypes.LocalizedText(u'Dräger'))
        mdsDescriptor.ModelName.append(pmtypes.LocalizedText(model.modelName[None]))
        mdsDescriptor.SerialNumber.append(pmtypes.ElementWithTextOnly('ABCD-1234'))
        mdsDescriptor.ModelNumber = '0.99'
        mdsDescriptor.updateNode()
        super(SomeDevice, self).__init__(wsdiscovery, my_uuid, model, device, deviceMdibContainer, validate,
                                         # registerDefaultOperations=True,
                                         useSSL=useSSL, sslContext=sslContext, logLevel=logLevel, log_prefix=log_prefix)
        #self._handler.mkDefaultRoleHandlers()
    @classmethod
    def fromMdibFile(cls, wsdiscovery, my_uuid, mdib_xml_path,
                 validate=True, useSSL=True, sslContext=None, logLevel=logging.INFO, log_prefix=''):
        """
        An alternative constructor for the class
        """
        if not os.path.isabs(mdib_xml_path):
            here = os.path.dirname(__file__)
            mdib_xml_path = os.path.join(here, mdib_xml_path)

        with open(mdib_xml_path, 'rb') as f:
            mdib_xml_string = f.read()
        return cls(wsdiscovery, my_uuid, mdib_xml_string, validate, useSSL, sslContext, logLevel, log_prefix=log_prefix)
