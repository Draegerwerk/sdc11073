import threading
import uuid
import copy
import time
import socket
import traceback
import http.client
from http.server import HTTPServer
import queue
import urllib
from lxml import etree as etree_
from sdc11073.pysoap.soapenvelope import Soap12Envelope, ReceivedSoap12Envelope, WsAddress, WsSubscribe, \
    SoapResponseException
from sdc11073.pysoap.soapenvelope import WsaEndpointReferenceType
from ..namespaces import Prefix_Namespace as Prefix
from ..namespaces import nsmap as _global_nsmap
from ..namespaces import wseTag, wsaTag, WSA_IS_REFERENCE_PARAMETER
from .. import xmlparsing, isoduration
from .. import commlog
from .. import observableproperties as properties
from .. import loghelper
from ..httprequesthandler import HTTPRequestHandler
from sdc11073.pysoap.soapclient import HTTPReturnCodeError

MULTITHREADED = True
SUBSCRIPTION_CHECK_INTERVAL = 5  # seconds


class MyThreadingMixIn(object):

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.

        In addition, exception handling is done here.

        """
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except Exception as ex:
            if self.dispatcher is not None:
                # only
                self.handle_error(request, client_address)
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        t = threading.Thread(target=self.process_request_thread,
                             args=(request, client_address),
                             name='SubscrRecv{}'.format(client_address))
        t.daemon = True
        t.start()
        self.threads.append((t, request, client_address))


if MULTITHREADED:
    class MyHTTPServer(MyThreadingMixIn, HTTPServer):
        """ Each request is handled in a thread.
        Following receipe from https://pymotw.com/2/BaseHTTPServer/index.html#module-BaseHTTPServer
        """

        def __init__(self, *args, **kwargs):
            HTTPServer.__init__(self, *args, **kwargs)
            self.daemon_threads = True
            self.threads = []
            self.dispatcher = None

else:
    MyHTTPServer = HTTPServer  # single threaded, sequential operation


class ClSubscription(object):
    """ This class handles a subscription to an event source.
    It stores all key data of the subscription and can renew and unsubscribe this subscription."""
    notification = properties.ObservableProperty()
    IDENT_TAG = etree_.QName('http.local.com', 'MyClIdentifier')

    def __init__(self, dpwsHosted, actions, notification_url, endTo_url, ident, xml_validator):
        """
        @param serviceClient:
        @param filter_:
        @param notification_url: e.g. http://1.2.3.4:9999, or https://1.2.3.4:9999
        """
        self.dpwsHosted = dpwsHosted
        self._actions = actions
        self._filter = ' '.join(actions)
        self._notification_url = notification_url
        self.isSubscribed = False
        self.end_status = None  # if device sent a SubscriptionEnd message, this contains the status from the message
        self.end_reason = None  # if device sent a SubscriptionEnd message, this contains the reason from the message
        self.expireAt = None
        self.expire_minutes = None
        self.dev_reference_param = None
        self.notifyTo_identifier = etree_.Element(self.IDENT_TAG)
        self.notifyTo_identifier.text = uuid.uuid4().urn

        self._endTo_url = endTo_url
        self._endTo_identifier = etree_.Element(self.IDENT_TAG)
        self._endTo_identifier.text = uuid.uuid4().urn

        self._subscriptionManagerAddress = None
        self._logger = loghelper.getLoggerAdapter('sdc.client.subscr', ident)
        self.eventCounter = 0  # for display purpose, we count notifications
        self.cl_ident = ident
        self._device_epr = urllib.parse.urlparse(self.dpwsHosted.endpointReferences[0].address).path
        self._xml_validator = xml_validator

    @property
    def end_to_identifier(self):
        return self._endTo_identifier

    def _mkSubscribeEnvelope(self, subscribe_epr, expire_minutes):
        soapEnvelope = Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
        soapEnvelope.setAddress(WsAddress(action='http://schemas.xmlsoap.org/ws/2004/08/eventing/Subscribe',
                                          to=subscribe_epr))

        body = WsSubscribe(notifyTo=WsaEndpointReferenceType(self._notification_url,
                                                             referenceParametersNode=[self.notifyTo_identifier]),
                           endTo=WsaEndpointReferenceType(self._endTo_url,
                                                          referenceParametersNode=[self._endTo_identifier]),
                           expires=expire_minutes * 60,
                           filter_=self._filter)
        soapEnvelope.addBodyObject(body)
        return soapEnvelope

    def _handleSubscribeResponse(self, soapEnvelope):
        # Check content of response; raise Error if subscription was not successful
        try:
            msgNode = soapEnvelope.msgNode
            if msgNode.tag == wseTag('SubscribeResponse'):
                address = msgNode.xpath('wse:SubscriptionManager/wsa:Address/text()', namespaces=_global_nsmap)
                self.dev_reference_param = None

                reference_params = msgNode.xpath('wse:SubscriptionManager/wsa:ReferenceParameters',
                                                 namespaces=_global_nsmap)
                if reference_params:
                    self.dev_reference_param = reference_params[0]
                expires = msgNode.xpath('wse:Expires/text()', namespaces=_global_nsmap)

                self._subscriptionManagerAddress = urllib.parse.urlparse(address[0])
                expireseconds = isoduration.parse_duration(expires[0])
                self.expireAt = time.time() + expireseconds
                self.isSubscribed = True
                self._logger.info('Subscribe was successful: expires at {}, address="{}"',
                                  self.expireAt, self._subscriptionManagerAddress)
            else:
                # This is a failure response or even rubbish. log it and raise error
                self._logger.error('Subscribe response has unexpected content: {}', soapEnvelope.as_xml(pretty=True))
                self.isSubscribed = False
                raise SoapResponseException(soapEnvelope)
        except AttributeError:
            self._logger.error('Subscribe response has unexpected content: {}', soapEnvelope.as_xml(pretty=True))
            self.isSubscribed = False
            raise SoapResponseException(soapEnvelope)

    def subscribe(self, expire_minutes=60):
        self._logger.info('### startSubscription "{}" ###', self._filter)
        self.eventCounter = 0
        self.expire_minutes = expire_minutes  # saved for later renewal, we will use the same interval
        # ToDo: check if there is more than one address. In that case a clever selection is needed
        address = self.dpwsHosted.endpointReferences[0].address
        soapEnvelope = self._mkSubscribeEnvelope(address, expire_minutes)
        msg = 'subscribe {}'.format(self._filter)
        try:
            resultSoapEnvelope = self.dpwsHosted.soapClient.postSoapEnvelopeTo(self._device_epr, soapEnvelope, msg=msg)
            self._handleSubscribeResponse(resultSoapEnvelope)
        except HTTPReturnCodeError as ex:
            self._logger.error('could not subscribe: {}'.format(HTTPReturnCodeError))

    def _add_device_references(self, soapEnvelope):
        """ add references for requests to device (renew, getstatus, unsubscribe)"""
        if self.dev_reference_param is not None:
            for e in self.dev_reference_param:
                e_ = copy.copy(e)
                # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
                e_.set(wsaTag('IsReferenceParameter'), 'true')
                soapEnvelope.addHeaderElement(e_)

    def _mkRenewEnvelope(self, expire_minutes):
        soapEnvelope = Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
        soapEnvelope.setAddress(WsAddress(action='http://schemas.xmlsoap.org/ws/2004/08/eventing/Renew',
                                          to=urllib.parse.urlunparse(self._subscriptionManagerAddress)))
        self._add_device_references(soapEnvelope)
        renewNode = etree_.Element(wseTag('Renew'), nsmap=Prefix.partialMap(Prefix.WSE))
        expiresNode = etree_.SubElement(renewNode, wseTag('Expires'), nsmap=Prefix.partialMap(Prefix.WSE))
        expiresNode.text = isoduration.durationString(expire_minutes * 60)
        soapEnvelope.addBodyElement(renewNode)
        return soapEnvelope

    def _handleRenewResponse(self, soapEnvelope):
        # Check content of response; raise Error if subscription was not successful
        bodyNode = soapEnvelope.bodyNode
        renewResponse = bodyNode.xpath('wse:RenewResponse', namespaces=_global_nsmap)
        if len(renewResponse) == 1:
            # this means renew was accepted
            expires = bodyNode.xpath('wse:RenewResponse/wse:Expires/text()', namespaces=_global_nsmap)
            expireseconds = isoduration.parse_duration(expires[0])
            self.expireAt = time.time() + expireseconds
        else:
            raise SoapResponseException(soapEnvelope)

    def renew(self, expire_minutes=60):
        soapEnvelope = self._mkRenewEnvelope(expire_minutes)
        try:
            resultSoapEnvelope = self.dpwsHosted.soapClient.postSoapEnvelopeTo(self._subscriptionManagerAddress.path,
                                                                               soapEnvelope, msg='renew')
            self._logger.debug('{}', resultSoapEnvelope.as_xml(pretty=True))
        except HTTPReturnCodeError as ex:
            self.isSubscribed = False
            self._logger.error('could not renew: {}'.format(HTTPReturnCodeError))
        except (http.client.HTTPException, ConnectionError) as ex:
            self._logger.warn('renew failed: {}', ex)
            self.isSubscribed = False
        except Exception as ex:
            self._logger.error('Exception in renew: {}', ex)
            self.isSubscribed = False
        else:
            try:
                self._handleRenewResponse(resultSoapEnvelope)
                return self.remainingSubscriptionSeconds
            except SoapResponseException as ex:
                self.isSubscribed = False
                self._logger.warn('renew failed: {}',
                                  etree_.tostring(ex.soapResponseEnvelope.bodyNode, pretty_print=True))

    def unsubscribe(self):
        if not self.isSubscribed:
            return

        soapEnvelope = Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
        soapEnvelope.setAddress(WsAddress(action='http://schemas.xmlsoap.org/ws/2004/08/eventing/Unsubscribe',
                                          to=urllib.parse.urlunparse(self._subscriptionManagerAddress)))
        self._add_device_references(soapEnvelope)
        soapEnvelope.addBodyElement(etree_.Element(wseTag('Unsubscribe')))
        resultSoapEnvelope = self.dpwsHosted.soapClient.postSoapEnvelopeTo(self._subscriptionManagerAddress.path,
                                                                           soapEnvelope, msg='unsubscribe')
        responseAction = resultSoapEnvelope.address.action
        # check response: response does not contain explicit status. If action== UnsubscribeResponse all is fine.
        if responseAction == 'http://schemas.xmlsoap.org/ws/2004/08/eventing/UnsubscribeResponse':
            self._logger.info('unsubscribe: end of subscription {} was confirmed.', self._filter)
        else:
            self._logger.error('unsubscribe: unexpected response action: {}', resultSoapEnvelope.as_xml(pretty=True))
            raise RuntimeError(
                'unsubscribe: unexpected response action: {}'.format(resultSoapEnvelope.as_xml(pretty=True)))

    def _mkGetStatusEnvelope(self):
        soapEnvelope = Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
        soapEnvelope.setAddress(WsAddress(action='http://schemas.xmlsoap.org/ws/2004/08/eventing/GetStatus',
                                          to=urllib.parse.urlunparse(self._subscriptionManagerAddress)))
        self._add_device_references(soapEnvelope)
        bodyNode = etree_.Element(wseTag('GetStatus'))
        soapEnvelope.addBodyElement(bodyNode)
        return soapEnvelope

    def getStatus(self):
        """ Sends a GetStatus Request to the device.
        @return: the remaining time of the subscription or None, if the request was not successful
        """
        soapEnvelope = self._mkGetStatusEnvelope()
        try:
            resultSoapEnvelope = self.dpwsHosted.soapClient.postSoapEnvelopeTo(self._subscriptionManagerAddress.path,
                                                                               soapEnvelope, msg='getStatus')
        except HTTPReturnCodeError as ex:
            self.isSubscribed = False
            self._logger.error('could not get status: {}'.format(HTTPReturnCodeError))
        except (http.client.HTTPException, ConnectionError) as ex:
            self.isSubscribed = False
            self._logger.warn('getStatus: Connection Error {} for subscription {}', ex, self._filter)
        except Exception as ex:
            self._logger.error('Exception in getStatus: {}', ex)
            self.isSubscribed = False
        else:
            try:
                expiresNode = resultSoapEnvelope.msgNode.find('wse:Expires', namespaces=_global_nsmap)
                if expiresNode is None:
                    self._logger.warn('getStatus for {}: Could not find "Expires" node! getStatus={} ', self._filter,
                                      resultSoapEnvelope.rawdata)
                    raise SoapResponseException(resultSoapEnvelope)
                else:
                    expires = expiresNode.text
                    expiresValue = isoduration.parse_duration(expires)
                    self._logger.debug('getStatus for {}: Expires = {} = {} seconds, counter = {}', self._filter,
                                       expires,
                                       expiresValue,
                                       self.eventCounter)
                    return expiresValue
            except AttributeError:
                self._logger.warn('No msg in envelope')

    def checkStatus(self, renewLimit):
        """ Calls getStatus and updates internal data.
        @param renewLimit: a value in seconds. If remaining duration of subscription is less than this value, it renews the subscription.
        @return: None
        """
        if not self.isSubscribed:
            return

        remainingTime = self.getStatus()
        if remainingTime is None:
            self.isSubscribed = False
            return
        elif abs(remainingTime - self.remainingSubscriptionSeconds) > 10:
            self._logger.warn(
                'time delta between expected expire and reported expire  > 10 seconds. Will correct own expectation.')
            self.expireAt = time.time() + remainingTime

        if self.remainingSubscriptionSeconds < renewLimit:
            self._logger.info('renewing subscription')
            self.renew()

    def checkStatus_renew(self):
        """ Calls renew and updates internal data.
        @return: None
        """
        if self.isSubscribed:
            self.renew()

    @property
    def remainingSubscriptionSeconds(self):
        return self.expireAt - time.time()

    def onNotification(self, soapEnvelope):
        try:
            soapEnvelope.validate_envelope(self._xml_validator)
        except etree_.DocumentInvalid as ex:
            self._logger.error('received invalid document: {}', ex)
        except Exception as ex:
            self._logger.error('error validation document: {}', ex)
        self.eventCounter += 1
        self.notification = soapEnvelope

    @property
    def shortFilterString(self):
        return xmlparsing.shortFilterString(self._actions)

    def __str__(self):
        return 'Subscription of "{}", isSubscribed={}, remaining time = {} sec., count={}'.format(
            self.shortFilterString,
            self.isSubscribed,
            int(self.remainingSubscriptionSeconds),
            self.eventCounter)


class SubscriptionManager(threading.Thread):
    """ Factory for Subscription objects, thread that automatically renews expiring subscriptions.
    @param notification_url: the destination url for notifications.
    @param endTo_url: if given the destination url for end subscription notifications; if not given, the notification_url is used.
    @param check_interval: the interval (in seconds ) for getStatus requests. Defaults to SUBSCRIPTION_CHECK_INTERVAL
    @param ident: a string that is used in log output; defaults to empty string
     """
    allSubscriptionsOkay = properties.ObservableProperty(True)  # a boolean
    keepAlive_with_renew = True  # enable as workaround if checkstatus is not supported

    def __init__(self, notification_url, endTo_url=None, checkInterval=None, log_prefix='', xml_validator=None):
        super().__init__(name='Cl_SubscriptionManager{}'.format(log_prefix))
        self.daemon = True
        self._checkInterval = checkInterval or SUBSCRIPTION_CHECK_INTERVAL
        self.subscriptions = {}
        self._subscriptionsLock = threading.Lock()

        self._run = False
        self._notification_url = notification_url
        self._endTo_url = endTo_url or notification_url
        self._logger = loghelper.getLoggerAdapter('sdc.client.subscrMgr', log_prefix)
        self.log_prefix = log_prefix
        self._xml_validator = xml_validator

    def stop(self):
        self._run = False
        self.join(timeout=2)
        with self._subscriptionsLock:
            self.subscriptions.clear()

    def run(self):
        self._run = True
        try:
            while self._run:
                try:
                    for i in range(self._checkInterval):
                        time.sleep(1)
                        if not self._run:
                            return
                        # check if all subscriptions are okay
                        with self._subscriptionsLock:
                            not_okay = [s for s in self.subscriptions.values() if not s.isSubscribed]
                            subscribed = [s.isSubscribed for s in self.subscriptions.values()]
                            self.allSubscriptionsOkay = (len(not_okay) == 0)
                    with self._subscriptionsLock:
                        subscriptions = list(self.subscriptions.values())
                    for subscription in subscriptions:
                        if self.keepAlive_with_renew:
                            subscription.checkStatus_renew()
                        else:
                            subscription.checkStatus(renewLimit=self._checkInterval * 5)
                    self._logger.debug('##### SubscriptionManager Interval ######')
                    for subscription in subscriptions:
                        self._logger.debug('{}', subscription)
                except Exception as ex:
                    self._logger.error('##### check loop: {}', traceback.format_exc())
        finally:
            self._logger.info('terminating subscriptions check loop! self._run={}', self._run)

    def mkSubscription(self, dpwsHosted, filters):
        s = ClSubscription(dpwsHosted, filters, self._notification_url, self._endTo_url, self.log_prefix,
                           self._xml_validator)
        filter_ = ' '.join(filters)
        with self._subscriptionsLock:
            self.subscriptions[filter_] = s
        return s

    def onSubScriptionEnd(self, soapenvelope):
        subscr_ident_list = soapenvelope.headerNode.findall(ClSubscription.IDENT_TAG, namespaces=_global_nsmap)
        statuus = soapenvelope.bodyNode.xpath('wse:SubscriptionEnd/wse:Status/text()', namespaces=_global_nsmap)
        reasons = soapenvelope.bodyNode.xpath('wse:SubscriptionEnd/wse:Reason/text()', namespaces=_global_nsmap)
        if statuus:
            info = ' status={} '.format(statuus[0])
        else:
            info = ''
        if reasons:
            if len(reasons) == 1:
                info += ' reason = {}'.format(reasons[0])
            else:
                info += ' reasons = {}'.format(reasons)
        if not subscr_ident_list:
            self._logger.warn('onSubScriptionEnd: did not find any identifier in message')
            return None
        subscr_ident = subscr_ident_list[0]
        for s in self.subscriptions.values():
            if subscr_ident.text == s.end_to_identifier.text:
                self._logger.info('onSubScriptionEnd: received Subscription End for {} {}',
                                  s.shortFilterString,
                                  info)
                s.isSubscribed = False
                if len(statuus) > 0:
                    s.end_status = statuus[0]
                if len(reasons) > 0:
                    s.end_reason = reasons[0]
                return s
        self._logger.warn('onSubScriptionEnd: have no subscription for identifier = {}', subscr_ident.text)
        return None

    def unsubscribeAll(self):
        with self._subscriptionsLock:
            current_subscriptions = list(self.subscriptions.values())  # make a copy
            self.subscriptions.clear()
            for s in current_subscriptions:
                try:
                    s.unsubscribe()
                except:
                    self._logger.warn('unsubscribe error: {}\n call stack:{} ', traceback.format_exc(),
                                      traceback.format_stack())


class _DispatchError(Exception):
    def __init__(self, httpErrorcode, errorText):
        super(_DispatchError, self).__init__()
        self.httpErrorcode = httpErrorcode
        self.errorText = errorText


class SOAPNotificationsDispatcher(object):
    """ receiver of all notifications"""

    def __init__(self, log_prefix, sdc_definitions):
        self._logger = loghelper.getLoggerAdapter('sdc.client.notif_dispatch', log_prefix)
        self.log_prefix = log_prefix
        self._sdc_definitions = sdc_definitions
        self.methods = {}

    def register_function(self, action, ref, fn):
        self.methods[(ref, action)] = fn

    def dispatch(self, path, xml):
        start = time.time()

        fn, action, request = self._dispatch_process_data(xml, path)
        fn(request)

        duration = time.time() - start
        if duration > 0.005:
            self._logger.debug('action {}: duration = {:.4f}sec', action, duration)
        return ''

    def _dispatch_process_data(self, xml, path):
        normalized_xml = self._sdc_definitions.normalizeXMLText(xml)
        request = ReceivedSoap12Envelope.fromXMLString(normalized_xml)
        try:
            action = request.address.action
        except AttributeError:
            raise _DispatchError(404, 'no action in request')
        self._logger.debug('received notification path={}, action = {}', path, action)

        ref_parameters = request.headerNode.findall(ClSubscription.IDENT_TAG, namespaces=_global_nsmap)
        ref_parameters = [i for i in ref_parameters if i.get(WSA_IS_REFERENCE_PARAMETER) in ("1", "true")]

        if len(ref_parameters) != 1:
            raise _DispatchError(400, f'Expected exactly one matching reference parameter missing from request.')

        try:
            fn = self.methods[(ref_parameters[0].text, action)]
        except KeyError:
            self._logger.error('action "{}" not registered. Known:{}'.format(action, self.methods.keys()))
            raise _DispatchError(404, 'action not registered')

        return fn, action, request


class SOAPNotificationsDispatcherThreaded(SOAPNotificationsDispatcher):

    def __init__(self, ident, bicepsSchema):
        super(SOAPNotificationsDispatcherThreaded, self).__init__(ident, bicepsSchema)
        self._queue = queue.Queue(1000)
        self._worker = threading.Thread(target=self._readqueue)
        self._worker.daemon = True
        self._worker.start()

    def dispatch(self, path, xml):
        fn, action, request = self._dispatch_process_data(xml, path)
        self._queue.put((fn, request, action))
        return ''

    def _readqueue(self):
        while True:
            fn, request, action = self._queue.get()
            try:
                fn(request)
            except:
                self._logger.error(
                    'method {} for action "{}" failed:{}'.format(fn.__name__, action, traceback.format_exc()))


class SOAPNotificationsHandler(HTTPRequestHandler):
    disable_nagle_algorithm = True
    wbufsize = 0xffff  # 64k buffer to prevent tiny packages
    RESPONSE_COMPRESS_MINSIZE = 256  # bytes, compress response it it is larger than this value (and other side supports compression)

    def do_POST(self):
        """SOAP POST gateway"""
        self.server.threadObj._logger.debug('notification do_POST incoming')  # pylint: disable=protected-access
        dispatcher = self.server.dispatcher
        response_string = ''
        if dispatcher is None:
            # close this connection
            self.close_connection = 1
            self.server.threadObj._logger.warn(
                'received a POST request, but no dispatcher => returning 404 ')  # pylint:disable=protected-access
            self.send_response(404)  # not found
        else:
            request_bytes = self._read_request()

            self.server.threadObj._logger.debug('notification {} bytes',
                                                request_bytes)  # pylint: disable=protected-access
            # execute the method
            commlog.defaultLogger.logSoapSubscrMsgIn(request_bytes)
            try:
                response_string = self.server.dispatcher.dispatch(self.path, request_bytes)
                if response_string is None:
                    response_string = ''
                self.send_response(202, b'Accepted')
            except _DispatchError as ex:
                self.server.threadObj._logger.error('received a POST request, but got _DispatchError => returning {}',
                                                    ex.httpErrorcode)  # pylint:disable=protected-access
                self.send_response(ex.httpErrorcode, ex.errorText)
            except Exception as ex:
                self.server.threadObj._logger.error(
                    'received a POST request, but got Exception "{}"=> returning {}\n{}', ex, 500,
                    traceback.format_exc())  # pylint:disable=protected-access
                self.send_response(500, b'server error in dispatch')
        response_bytes = response_string.encode('utf-8')
        if len(response_bytes) > self.RESPONSE_COMPRESS_MINSIZE:
            response_bytes = self._compressIfRequired(response_bytes)

        self.send_header("Content-Type", "application/soap+xml; charset=utf-8")
        self.send_header("Content-Length", len(response_bytes))  # this is necessary for correct keep-alive handling!
        self.end_headers()
        self.wfile.write(response_bytes)


class NotificationsReceiverDispatcherThread(threading.Thread):

    def __init__(self, my_ipaddress, sslContext, log_prefix, sdc_definitions, supportedEncodings,
                 soap_notifications_handler_class=None, async_dispatch=True):
        """

        :param my_ipaddress: http server will listen on this address
        :param sslContext: http server uses this ssl context
        :param ident: used for logging
        :param sdc_definitions: namespaces etc
        :param supportedEncodings: a list of strings
        :param soap_notifications_handler_class: if None, SOAPNotificationsHandler is used,
                otherwise the provided class ( a HTTPRequestHandler).
        :param async_dispatch: if True, incoming requests are queued and response is sent (processing is done later).
                                if False, response is sent after the complete processing is done.
        """
        super(NotificationsReceiverDispatcherThread, self).__init__(
            name='Cl_NotificationsReceiver_{}'.format(log_prefix))
        self._sslContext = sslContext
        self._soap_notifications_handler_class = soap_notifications_handler_class
        self.daemon = True
        self._logger = loghelper.getLoggerAdapter('sdc.client.notif_dispatch', log_prefix)

        self._my_ipaddress = my_ipaddress
        self.my_port = None
        self.base_url = None
        self.httpd = None
        self.supportedEncodings = supportedEncodings
        # create and set up the dispatcher for notifications
        if async_dispatch:
            self.dispatcher = SOAPNotificationsDispatcherThreaded(log_prefix, sdc_definitions)
        else:
            self.dispatcher = SOAPNotificationsDispatcher(log_prefix, sdc_definitions)
        self.started_evt = threading.Event()  # helps to wait until thread has initialised is variables

    def run(self):
        try:
            myport = 0  # zero means that OS selects a free port
            self.httpd = MyHTTPServer((self._my_ipaddress, myport),
                                      self._soap_notifications_handler_class or SOAPNotificationsHandler)
            # add use compression flag to the server
            setattr(self.httpd, 'supportedEncodings', self.supportedEncodings)
            self.my_port = self.httpd.server_port
            self._logger.info('starting Notification receiver on {}:{}', self._my_ipaddress, self.my_port)
            if self._sslContext:
                self.httpd.socket = self._sslContext.wrap_socket(self.httpd.socket, do_handshake_on_connect=False)
                self.base_url = 'https://{}:{}/'.format(self._my_ipaddress, self.my_port)
            else:
                self.base_url = 'http://{}:{}/'.format(self._my_ipaddress, self.my_port)
            self.httpd.dispatcher = self.dispatcher
            self.httpd.threadObj = self  # make logger available for SOAPNotificationsHandler
            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            self._logger.error(
                'Unhandled Exception at thread runtime. Thread will abort! {}'.format(traceback.format_exc()))
            raise

    def stop(self, closeAllConnections=True):
        """
        @param closeAllConnections: for testing purpose one might want to keep the connection handler threads alive.
                If param is False then they are kept alive.
        """
        self.httpd.shutdown()
        self.httpd.socket.close()
        if closeAllConnections:
            if self.httpd.dispatcher is not None:
                self.httpd.dispatcher.methods = {}
                self.httpd.dispatcher = None  # this leads to a '503' reaction in SOAPNotificationsHandler
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    try:
                        request.shutdown(socket.SHUT_RDWR)
                        request.close()
                        self._logger.info('closed socket for notifications from {}', client_addr)
                    except OSError as ex:
                        # the connection is already closed
                        continue
                    except Exception as ex:
                        self._logger.warn('error closing socket for notifications from {}: {}', client_addr, ex)
            time.sleep(0.1)
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    thread.join(2)
            del self.httpd.threads[:]
