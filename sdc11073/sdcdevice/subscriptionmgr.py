import uuid
import time
import copy
import socket
import traceback
from collections import deque, defaultdict
import urllib
import http.client

from lxml import etree as etree_
from ..namespaces import xmlTag, wseTag, wsaTag, msgTag, nsmap, DocNamespaceHelper
from ..namespaces import Prefixes
from .. import isoduration
from .. import xmlparsing
from .. import observableproperties
from .. import multikey
from .. import loghelper
from ..compression import CompressionHandler
from ..pysoap.soapenvelope import Soap12Envelope, SoapFault, WsAddress, WsaEndpointReferenceType
from ..pysoap.soapclient import SoapClient, HTTPReturnCodeError

MAX_ROUNDTRIP_VALUES = 20


class _RoundTripData:
    def __init__(self, values, abs_max):
        if values:
            self.values = list(values)  # make a copy
            self.min = min(values)
            self.max = max(values)
            self.avg = sum(values) / len(values)
            self.abs_max = abs_max
        else:
            self.values = None
            self.min = None
            self.max = None
            self.avg = None
            self.abs_max = None

    def __repr__(self):
        return 'min={:.4f} max={:.4f} avg={:.4f} absmax={:.4f}'.format(self.min, self.max, self.avg, self.abs_max)


class _DevSubscription:
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, mode, base_urls, notifyToAddress, notifyRefNode, endToAddress, endToRefNode, expires,
                 max_subscription_duration, filter_, sslContext, biceps_schema,
                 acceptedEncodings):  # pylint:disable=too-many-arguments
        """
        @param notifyToAddress: dom node of Subscribe Request
        @param endToAddress: dom node of Subscribe Request
        @param expires: seconds as float
        @param filter: a space separated list of actions, or only one action
        """
        self.mode = mode
        self.base_urls = base_urls
        self.notifyToAddress = notifyToAddress
        self._url = urllib.parse.urlparse(notifyToAddress)

        self.notifyRefNodes = []
        if notifyRefNode is not None:
            self.notifyRefNodes = list(notifyRefNode)  # all children

        self.endToAddress = endToAddress
        self.endToRefNodes = []
        if endToRefNode is not None:
            self.endToRefNodes = list(endToRefNode)  # all children
        self.my_identifier = etree_.Element(self.IDENT_TAG)
        self.my_identifier.text = uuid.uuid4().urn

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expireseconds = None
        self.renew(expires)  # sets self._started and self._expireseconds
        self._filters = filter_.split()
        self._ssl_context = sslContext
        self._bicepsSchema = biceps_schema

        self._acceptedEncodings = acceptedEncodings  # these encodings does the other side accept
        self._soapClient = None

        self._notifyErrors = 0
        self._is_closed = False
        self._isConnectionError = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES)  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0

    def setSoapClient(self, soapClient):
        self._soapClient = soapClient

    def renew(self, expires):
        self._started = time.monotonic()
        if expires:
            self._expireseconds = min(expires, self._max_subscription_duration)
        else:
            self._expireseconds = self._max_subscription_duration

    @property
    def soapClient(self):
        return self._soapClient

    @property
    def remainingSeconds(self):
        duration = int(self._expireseconds - (time.monotonic() - self._started))
        return 0 if duration < 0 else duration

    @property
    def expireString(self):
        return isoduration.duration_string(self.remainingSeconds)

    @property
    def hasDeliveryFailure(self):
        return self._notifyErrors >= self.MAX_NOTIFY_ERRORS

    @property
    def hasConnectionError(self):
        return self._isConnectionError

    @property
    def isValid(self):
        if self._is_closed:
            return False
        return self.remainingSeconds > 0 and not self.hasDeliveryFailure

    def matches(self, action):
        action = action.strip()  # just to be sure there are no spaces....
        for f in self._filters:
            if f.endswith(action):
                return True
        return False

    def _mkNotificationReport(self, soapEnvelope, action):
        addr = WsAddress(to=self.notifyToAddress,
                         action=action,
                         from_=None,
                         replyTo=None,
                         faultTo=None,
                         referenceParametersNode=None)
        soapEnvelope.setAddress(addr)
        for identNode in self.notifyRefNodes:
            soapEnvelope.addHeaderElement(identNode)
        soapEnvelope.validateBody(self._bicepsSchema.message_schema)
        return soapEnvelope

    def _mkEndReport(self, soapEnvelope, action):
        to_addr = self.endToAddress or self.notifyToAddress
        addr = WsAddress(to=to_addr,
                         action=action,
                         from_=None,
                         replyTo=None,
                         faultTo=None,
                         referenceParametersNode=None)
        soapEnvelope.setAddress(addr)
        ref_nodes = self.endToRefNodes or self.notifyRefNodes
        for identNode in ref_nodes:
            identNode_ = copy.copy(identNode)
            # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
            identNode_.set(wsaTag('IsReferenceParameter'), 'true')
            soapEnvelope.addHeaderElement(identNode_)
        return soapEnvelope

    # def sendNotificationReport(self, bodyNode, action, doc_nsmap):
    #     if not self.isValid:
    #         return
    #     soapEnvelope = Soap12Envelope(doc_nsmap)
    #     soapEnvelope.addBodyElement(bodyNode)
    #     rep = self._mkNotificationReport(soapEnvelope, action)
    #     try:
    #         roundtrip_timer = observableproperties.SingleValueCollector(self._soapClient, 'roundtrip_time')
    #
    #         self._soapClient.postSoapEnvelopeTo(self._url.path, rep, responseFactory=lambda x, schema: x,
    #                                             msg='sendNotificationReport {}'.format(action))
    #         try:
    #             roundtrip_time = roundtrip_timer.result(0)
    #             self.last_roundtrip_times.append(roundtrip_time)
    #             self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
    #         except observableproperties.CollectTimeoutError:
    #             pass
    #         self._notifyErrors = 0
    #         self._isConnectionError = False
    #     except HTTPReturnCodeError:
    #         self._notifyErrors += 1
    #         raise
    #     except Exception:  # any other exception is handled as an unreachable location (disconnected)
    #         self._notifyErrors += 1
    #         self._isConnectionError = True
    #         raise
    def sendNotificationReport(self, msg_factory, body_node, action, doc_nsmap):
        if not self.isValid:
            return
        addr = WsAddress(to=self.notifyToAddress,
                         action=action,
                         from_=None,
                         replyTo=None,
                         faultTo=None,
                         referenceParametersNode=None)
        soap_envelope = msg_factory.mk_notification_report(addr, body_node, self.notifyRefNodes, doc_nsmap)
        try:
            roundtrip_timer = observableproperties.SingleValueCollector(self._soapClient, 'roundtrip_time')

            self._soapClient.postSoapEnvelopeTo(self._url.path, soap_envelope, responseFactory=lambda x, schema: x,
                                                msg='sendNotificationReport {}'.format(action))
            try:
                roundtrip_time = roundtrip_timer.result(0)
                self.last_roundtrip_times.append(roundtrip_time)
                self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
            except observableproperties.CollectTimeoutError:
                pass
            self._notifyErrors = 0
            self._isConnectionError = False
        except HTTPReturnCodeError:
            self._notifyErrors += 1
            raise
        except Exception:  # any other exception is handled as an unreachable location (disconnected)
            self._notifyErrors += 1
            self._isConnectionError = True
            raise

    def sendNotificationEndMessage(self, action, code='SourceShuttingDown', reason='Event source going off line.'):
        doc_nsmap = DocNamespaceHelper().doc_ns_map
        my_addr = '{}:{}/{}'.format(self.base_urls[0].scheme, self.base_urls[0].netloc, self.base_urls[0].path)

        if not self.isValid:
            return
        if self._soapClient is None:
            return
        soapEnvelope = Soap12Envelope(doc_nsmap)

        subscriptionEndNode = etree_.Element(wseTag('SubscriptionEnd'),
                                             nsmap=Prefixes.partial_map(Prefixes.WSE, Prefixes.WSA, Prefixes.XML))
        subscriptionManagerNode = etree_.SubElement(subscriptionEndNode, wseTag('SubscriptionManager'))
        # child of Subscriptionmanager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        referenceParametersNode = etree_.Element(wsaTag('ReferenceParameters'))
        referenceParametersNode.append(copy.copy(self.my_identifier))
        epr = WsaEndpointReferenceType(address=my_addr, referenceParametersNode=referenceParametersNode)
        epr.as_etree_subnode(subscriptionManagerNode)

        # remark: optionally one could add own address and identifier here ...
        statusNode = etree_.SubElement(subscriptionEndNode, wseTag('Status'))
        statusNode.text = 'wse:{}'.format(code)
        reasonNode = etree_.SubElement(subscriptionEndNode, wseTag('Reason'),
                                       attrib={xmlTag('lang'): 'en-US'})
        reasonNode.text = reason

        soapEnvelope.addBodyElement(subscriptionEndNode)
        rep = self._mkEndReport(soapEnvelope, action)
        try:
            self._soapClient.postSoapEnvelopeTo(self._url.path, rep, responseFactory=lambda x, schema: x,
                                                msg='sendNotificationEndMessage {}'.format(action))
            self._notifyErrors = 0
            self._isConnectionError = False
            self._is_closed = True
        except Exception:
            # it does not matter that we could not send the message - end is end ;)
            pass

    def close(self):
        self._is_closed = True

    def isClosed(self):
        return self._is_closed

    def __repr__(self):
        try:
            refIdent = ', '.join([node.text for node in self.notifyRefNodes])
        except TypeError:
            refIdent = '<unknown>'
        return 'Subscription(notifyTo={} idnt={}, my_identifier={}, expires={}, filter={})'.format(self.notifyToAddress,
                                                                                                   refIdent,
                                                                                                   self.my_identifier.text,
                                                                                                   self.remainingSeconds,
                                                                                                   xmlparsing.short_filter_string(
                                                                                                       self._filters))

    @classmethod
    def fromSoapEnvelope(cls, soapEnvelope, sslContext, biceps_schema, acceptedEncodings, max_subscription_duration,
                         base_urls):
        endToAddress = None
        endToRefNode = []
        endToAddresses = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:EndTo', namespaces=nsmap)
        if len(endToAddresses) == 1:
            endToNode = endToAddresses[0]
            endToAddress = endToNode.xpath('wsa:Address/text()', namespaces=nsmap)[0]
            endToRefNode = endToNode.find('wsa:ReferenceParameters', namespaces=nsmap)

        # determine (mandatory) notification address
        deliveryNode = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:Delivery', namespaces=nsmap)[0]
        notifyToNode = deliveryNode.find('wse:NotifyTo', namespaces=nsmap)
        notifyToAddress = notifyToNode.xpath('wsa:Address/text()', namespaces=nsmap)[0]
        notifyRefNode = notifyToNode.find('wsa:ReferenceParameters', namespaces=nsmap)

        mode = deliveryNode.get('Mode')  # mandatory attribute

        expiresNodes = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:Expires/text()', namespaces=nsmap)
        if len(expiresNodes) == 0:
            expires = None
        else:
            expires = isoduration.parse_duration(str(expiresNodes[0]))

        filter_ = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:Filter/text()', namespaces=nsmap)[0]

        return cls(str(mode), base_urls, notifyToAddress, notifyRefNode, endToAddress, endToRefNode,
                   expires, max_subscription_duration, str(filter_), sslContext, biceps_schema, acceptedEncodings)

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        else:
            return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self._filters])


class SubscriptionsManager:
    BodyNodePrefixes = [Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML]
    NotificationPrefixes = [Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE]
    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription

    def __init__(self, sslContext, sdc_definitions, bicepsParser, msg_factory, supported_encodings,
                 max_subscription_duration=None, log_prefix=None, chunked_messages=False):
        self._ssl_context = sslContext
        self.bicepsParser = bicepsParser
        self.sdc_definitions = sdc_definitions
        self._msg_factory = msg_factory
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', self.log_prefix)
        self._chunked_messages = chunked_messages
        self.soapClients = {}  # key: net location, value soapClient instance
        self._supportedEncodings = supported_encodings
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.my_identifier.text))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj._url.netloc))  # pylint:disable=protected-access
        self.base_urls = None

    def setBaseUrls(self, base_urls):
        self.base_urls = base_urls

    def onSubscribeRequest(self, httpHeader, soapEnvelope, epr_path):
        acceptedEncodings = CompressionHandler.parse_header(httpHeader.get('Accept-Encoding'))
        s = _DevSubscription.fromSoapEnvelope(soapEnvelope, self._ssl_context, self.bicepsParser, acceptedEncodings,
                                              self._max_subscription_duration, self.base_urls)

        # assign a soap client
        key = s._url.netloc  # pylint:disable=protected-access
        soapClient = self.soapClients.get(key)
        if soapClient is None:
            soapClient = SoapClient(key, loghelper.get_logger_adapter('sdc.device.soap', self.log_prefix),
                                    sslContext=self._ssl_context, sdc_definitions=self.sdc_definitions,
                                    supported_encodings=self._supportedEncodings,
                                    requestEncodings=acceptedEncodings,
                                    chunked_requests=self._chunked_messages)
            self.soapClients[key] = soapClient
        s.setSoapClient(soapClient)
        with self._subscriptions.lock:
            self._subscriptions.add_object(s)
        self._logger.info('new {}', s)

        response = Soap12Envelope(Prefixes.partial_map(*self.NotificationPrefixes))
        replyAddress = soapEnvelope.address.mkReplyAddress(
            'http://schemas.xmlsoap.org/ws/2004/08/eventing/SubscribeResponse')
        response.addHeaderObject(replyAddress)
        subscribeResponseNode = etree_.Element(wseTag('SubscribeResponse'))
        subscriptionManagerNode = etree_.SubElement(subscribeResponseNode, wseTag('SubscriptionManager'))
        # child of Subscriptionmanager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        referenceParametersNode = etree_.Element(wsaTag('ReferenceParameters'))
        referenceParametersNode.append(copy.copy(s.my_identifier))
        if epr_path.startswith('/'):
            epr_path = epr_path[1:]
        my_addr = '{}://{}/{}'.format(self.base_urls[0].scheme, self.base_urls[0].netloc, epr_path)
        epr = WsaEndpointReferenceType(address=my_addr, referenceParametersNode=referenceParametersNode)
        epr.as_etree_subnode(subscriptionManagerNode)
        expiresNode = etree_.SubElement(subscribeResponseNode, wseTag('Expires'))
        expiresNode.text = s.expireString  # simply confirm request
        response.addBodyElement(subscribeResponseNode)
        self._logger.debug('onSubscribeRequest returns {}', lambda: response.as_xml(pretty=False))
        return response

    def onUnsubscribeRequest(self, soapEnvelope):
        ident = soapEnvelope.headerNode.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        if ident is not None:
            identtext = ident.text
            s = self._subscriptions.identifier.getOne(identtext, allowNone=True)
            if s is None:
                self._logger.warn('unsubscribe: no object found for id={}', identtext)
            else:
                s.close()
                with self._subscriptions.lock:
                    self._subscriptions.remove_object(s)
                self._logger.info('unsubscribe: object found and removed (Xaddr = {}, filter = {})', s.notifyToAddress,
                                  s._filters)  # pylint: disable=protected-access
                # now check if we can close the soap client
                key = s._url.netloc  # pylint: disable=protected-access
                subscriptionsWithSameSoapClient = self._subscriptions.netloc.get(key, [])
                if len(subscriptionsWithSameSoapClient) == 0:
                    self.soapClients[key].close()
                    del self.soapClients[key]
                    self._logger.info('unsubscribe: closed soap client to {})', key)
        else:
            self._logger.error('unsubscribe request did not contain an identifier!!!: {}',
                               soapEnvelope.as_xml(pretty=True))

        response = Soap12Envelope(nsmap)
        replyAddress = soapEnvelope.address.mkReplyAddress(
            'http://schemas.xmlsoap.org/ws/2004/08/eventing/UnsubscribeResponse')
        response.addHeaderObject(replyAddress)
        # response has empty body
        return response

    def notifyOperation(self, sequence_id, mdib_version, transaction_id, operation, invocation_state, error=None,
                        error_message=None):
        operation_handle_ref = operation.handle
        self._logger.info(
            'notifyOperation transaction={} operationHandleRef={}, operationState={}, error={}, errorMessage={}',
            transaction_id, operation_handle_ref, invocation_state, error, error_message)
        action = self.sdc_definitions.Actions.OperationInvokedReport
        subscribers = self._getSubscriptionsForAction(action)

        ns_map = Prefixes.partial_map(Prefixes.MSG, Prefixes.PM)
        body_bode = self._msg_factory.mk_operation_invoked_report_body(ns_map, mdib_version, sequence_id,
                                                                       operation_handle_ref, transaction_id,
                                                                       invocation_state, error, error_message)
        for s in subscribers:
            self._logger.info('notifyOperation: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_bode, action, Prefixes.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def onGetStatusRequest(self, soapEnvelope):
        self._logger.debug('onGetStatusRequest {}', lambda: soapEnvelope.as_xml(pretty=True))
        subscr = self._getSubscriptionforRequest(soapEnvelope)
        if subscr is None:
            response = SoapFault(soapEnvelope,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('InvalidMessage')
                                 )

        else:
            response = Soap12Envelope(Prefixes.partial_map(*self.NotificationPrefixes))
            replyAddress = soapEnvelope.address.mkReplyAddress(
                'http://schemas.xmlsoap.org/ws/2004/08/eventing/GetStatusResponse')
            response.addHeaderObject(replyAddress)
            renewResponseNode = etree_.Element(wseTag('GetStatusResponse'))
            expiresNode = etree_.SubElement(renewResponseNode, wseTag('Expires'))
            expiresNode.text = subscr.expireString  # simply confirm request
            response.addBodyElement(renewResponseNode)
        return response

    def onRenewRequest(self, soapEnvelope):
        identifierNode = soapEnvelope.headerNode.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        expires = soapEnvelope.bodyNode.xpath('wse:Renew/wse:Expires/text()', namespaces=nsmap)
        if len(expires) == 0:
            expires = None
            self._logger.debug('onRenewRequest: no requested duration found, allowing max. ',
                               lambda: soapEnvelope.as_xml(pretty=True))
        else:
            expires = isoduration.parse_duration(str(expires[0]))
            self._logger.debug('onRenewRequest {} seconds', expires)

        subscr = self._getSubscriptionforRequest(soapEnvelope)
        if subscr is None:
            response = SoapFault(soapEnvelope,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('UnableToRenew')
                                 )

        else:
            subscr.renew(expires)

            response = Soap12Envelope(Prefixes.partial_map(*self.NotificationPrefixes))
            replyAddress = soapEnvelope.address.mkReplyAddress(
                'http://schemas.xmlsoap.org/ws/2004/08/eventing/RenewResponse')
            response.addHeaderObject(replyAddress)
            renewResponseNode = etree_.Element(wseTag('RenewResponse'))
            expiresNode = etree_.SubElement(renewResponseNode, wseTag('Expires'))
            expiresNode.text = subscr.expireString
            response.addBodyElement(renewResponseNode)
        return response

    def sendEpisodicMetricReport(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicMetricReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic metric report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_metric_report_body(
            states, ns_map, mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendEpisodicMetricReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendPeriodicMetricReport(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicMetricReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic metric report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_metric_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendPeriodicMetricReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendEpisodicOperationalStateReport(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicOperationalStateReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic operational state report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_operational_state_report_body(
            states, ns_map, mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendEpisodicOperationalStateReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendPeriodicOperationalStateReport(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicOperationalStateReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic operational state report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_operational_state_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendPeriodicOperationalStateReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendEpisodicAlertReport(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicAlertReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic alert report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_alert_report_body(
            states, ns_map, mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendEpisodicAlertReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendPeriodicAlertReport(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicAlertReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic alert report, contains last {} episodic updates', len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_alert_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendPeriodicAlertReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendEpisodicComponentStateReport(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicComponentReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic component report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_component_state_report_body(
            states, ns_map, mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendEpisodicComponentStateReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendPeriodicComponentStateReport(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicComponentReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic component report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_component_state_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendPeriodicComponentStateReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendEpisodicContextReport(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicContextReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic context report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_context_report_body(
            states, ns_map, mdib_version, sequence_id)

        for s in subscribers:
            self._logger.info('sendEpisodicContextReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendPeriodicContextReport(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicContextReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic context report, contains last {} episodic updates', len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_context_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)

        for s in subscribers:
            self._logger.debug('sendPeriodicContextStateReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def sendRealtimeSamplesReport(self, realtime_sample_states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.Waveform
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_realtime_samples_report_body(
            realtime_sample_states, ns_map, mdib_version, sequence_id)
        # bodyNode = etree_.Element(msgTag('WaveformStream'),
        #                           attrib={'SequenceId': sequence_id,
        #                                   'MdibVersion': str(mdib_version)},
        #                           nsmap=nsmapper.partial_map(*self.BodyNodePrefixes))
        #
        # for s in updatedRealTimeSampleStates:
        #     stateNode = s.mk_state_node(msgTag('State'),set_xsi_type=False)
        #     bodyNode.append(stateNode)

        for s in subscribers:
            self._logger.debug('sendRealtimeSamplesReport: sending report to {}', s.notifyToAddress)
            self._sendNotificationReport(s, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def endAllSubscriptions(self, sendSubscriptionEnd):
        action = self.sdc_definitions.Actions.SubscriptionEnd
        with self._subscriptions.lock:
            if sendSubscriptionEnd:
                for s in self._subscriptions.objects:
                    s.sendNotificationEndMessage(action)
            self._subscriptions.clear()

    def _mkDescriptorUpdatesReportPart(self, parent_node, modificationtype, descriptors, updated_states):
        """ Helper that creates ReportPart."""
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        for descrContainer in descriptors:
            reportPart = etree_.SubElement(parent_node, msgTag('ReportPart'),
                                           attrib={'ModificationType': modificationtype})
            if descrContainer.parent_handle is not None:  # only Mds can have None
                reportPart.set('ParentDescriptor', descrContainer.parent_handle)
            node = descrContainer.mk_descriptor_node(tag=msgTag('Descriptor'))
            reportPart.append(node)
            relatedStateContainers = [s for s in updated_states if s.descriptorHandle == descrContainer.handle]
            for stateContainer in relatedStateContainers:
                node = stateContainer.mk_state_node(msgTag('State'))
                reportPart.append(node)

    def sendDescriptorUpdates(self, updated, created, deleted, updated_states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.DescriptionModificationReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        bodyNode = etree_.Element(msgTag('DescriptionModificationReport'),
                                  attrib={'SequenceId': sequence_id,
                                          'MdibVersion': str(mdib_version)},
                                  nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM))
        self._mkDescriptorUpdatesReportPart(bodyNode, 'Upt', updated, updated_states)
        self._mkDescriptorUpdatesReportPart(bodyNode, 'Crt', created, updated_states)
        self._mkDescriptorUpdatesReportPart(bodyNode, 'Del', deleted, updated_states)

        for s in subscribers:
            self._sendNotificationReport(s, bodyNode, action,
                                         nsmapper.partial_map(*self.NotificationPrefixes))
        self._doHousekeeping()

    def _sendNotificationReport(self, subscription, bodyNode, action, doc_nsmap):
        try:
            subscription.sendNotificationReport(self._msg_factory, bodyNode, action, doc_nsmap)
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: HTTP status= {}, reason={}, {}', ex.status,
                               ex.reason, subscription)
        except http.client.NotConnected as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)
        except socket.timeout as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
        except etree_.DocumentInvalid as ex:
            # this is an error related to the document, it cannot be sent to any subscriber => re-raise
            self._logger.error('Invalid Document: {!r}\n{}', ex, etree_.tostring(bodyNode))
            raise
        except Exception as ex:
            # this should never happen! => re-raise
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)

    def _getSubscriptionsForAction(self, action):
        with self._subscriptions.lock:
            return [s for s in self._subscriptions.objects if s.matches(action)]

    def _getSubscriptionforRequest(self, soapEnvelope):
        request_name = soapEnvelope.bodyNode[0].tag
        identifierNode = soapEnvelope.headerNode.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        if identifierNode is None:
            raise RuntimeError('no Identifier found in {} ', request_name)
        else:
            identifier = identifierNode.text
        with self._subscriptions.lock:
            subscr = [s for s in self._subscriptions.objects if s.my_identifier.text == identifier]
        if len(subscr) > 1:
            raise RuntimeError('Have {} subscriptions with identifier "{}"!'.format(len(subscr), identifier))
        elif len(subscr) == 0:
            self._logger.error('on {}: unknown Subscription identifier "{}"', request_name, identifier)
            return
        return subscr[0]

    def _doHousekeeping(self):
        """ remove expired or invalid subscriptions"""
        with self._subscriptions._lock:  # pylint: disable=protected-access
            crap = [s for s in self._subscriptions.objects if not s.isValid]
        unreachable_netlocs = []
        for c in crap:
            if c.hasConnectionError:
                # the network location is unreachable, we can remove all subscriptions that use this location
                unreachable_netlocs.append(c.soapClient.netloc)
                try:
                    c.soapClient.close()
                except:
                    self._logger.error('error in soapClient.close(): {}', traceback.format_exc())

            self._logger.info('deleting {}, errors={}', c, c._notifyErrors)  # pylint: disable=protected-access
            with self._subscriptions.lock:
                self._subscriptions.remove_object(c)

            if c.soapClient.netloc in self.soapClients:  # remove closed soap client from list
                del self.soapClients[c.soapClient.netloc]

        # now find all subscriptions that have the same address
        with self._subscriptions._lock:  # pylint: disable=protected-access
            also_unreachable = [s for s in self._subscriptions.objects if
                                s.soapClient is not None and s.soapClient.netloc in unreachable_netlocs]
            for s in also_unreachable:
                self._logger.info('deleting also subscription {}, same endpoint', s)
                self._subscriptions.remove_object(s)

    def getSubScriptionRoundtripTimes(self):
        """Calculates roundtrip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=(<notifyToAddress>, (subscriptionnames)), value = _RoundTripData with members min, max, avg, abs_max, values
        """
        ret = {}
        with self._subscriptions.lock:
            for s in self._subscriptions.objects:
                if s.max_roundtrip_time > 0:
                    ret[(s.notifyToAddress, s.short_filter_names())] = s.get_roundtrip_stats()
        return ret

    def getClientRoundtripTimes(self):
        """Calculates roundtrip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=<notifyToAddress>, value = _RoundTripData with members min, max, avg, abs_max, values
        """
        # first step: collect all roundtrip times of subscriptions, group them by notifyToAddress
        tmp = defaultdict(list)
        ret = {}
        with self._subscriptions.lock:
            for s in self._subscriptions.objects:
                if s.max_roundtrip_time > 0:
                    tmp[s.notifyToAddress].append(s.get_roundtrip_stats())
        for k, stats in tmp.items():
            allvalues = []
            for s in stats:
                allvalues.extend(s.values)
            ret[k] = _RoundTripData(allvalues, max([s.max for s in stats]), )
        return ret
