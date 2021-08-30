import copy
import http.client
import queue
import socket
import threading
import time
import traceback
import urllib
import uuid
from http.server import HTTPServer

from lxml import etree as etree_

from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.pysoap.soapenvelope import ReceivedSoap12Envelope, SoapResponseException
from .. import commlog
from .. import loghelper
from .. import observableproperties as properties
from .. import etc, isoduration
from ..httprequesthandler import HTTPRequestHandler
from ..namespaces import nsmap as _global_nsmap
from ..namespaces import wseTag, wsaTag

MULTITHREADED = True
SUBSCRIPTION_CHECK_INTERVAL = 5  # seconds


class MyThreadingMixIn:

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.

        In addition, exception handling is done here.

        """
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except Exception as ex:
            if self.dispatcher is not None:
                self.handle_error(request, client_address)
            else:
                print("don't care error:{}".format(ex))
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        thread = threading.Thread(target=self.process_request_thread,
                                  args=(request, client_address),
                                  name='SubscrRecv{}'.format(client_address))
        thread.daemon = True
        thread.start()
        self.threads.append((thread, request, client_address))


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


class _ClSubscription:
    """ This class handles a subscription to an event source.
    It stores all key data of the subscription and can renew and unsubscribe this subscription."""
    notification = properties.ObservableProperty()
    IDENT_TAG = etree_.QName('http.local.com', 'MyClIdentifier')

    def __init__(self, msg_factory, dpws_hosted, actions, notification_url, end_to_url, ident):
        """
        @param serviceClient:
        @param filter_:
        @param notification_url: e.g. http://1.2.3.4:9999, or https://1.2.3.4:9999
        """
        self._msg_factory = msg_factory
        self.dpws_hosted = dpws_hosted
        self._actions = actions
        self._filter = ' '.join(actions)
        self._notification_url = notification_url
        self.is_subscribed = False
        self.expire_at = None
        self.expire_minutes = None
        self.dev_reference_param = None
        self.notify_to_identifier = etree_.Element(self.IDENT_TAG)
        self.notify_to_identifier.text = uuid.uuid4().urn

        self._end_to_url = end_to_url
        self.end_to_identifier = etree_.Element(self.IDENT_TAG)
        self.end_to_identifier.text = uuid.uuid4().urn

        self._subscription_manager_address = None
        self._logger = loghelper.get_logger_adapter('sdc.client.subscr', ident)
        self.event_counter = 0  # for display purpose, we count notifications
        self.cl_ident = ident
        self._device_epr = urllib.parse.urlparse(self.dpws_hosted.endpoint_references[0].address).path

    def _mk_subscribe_envelope(self, subscribe_epr, expire_minutes):
        return self._msg_factory.mk_subscribe_envelope(
            subscribe_epr, self._notification_url, self.notify_to_identifier,
            self._end_to_url, self.end_to_identifier, expire_minutes, self._filter)

    def _handle_subscribe_response(self, envelope):
        # Check content of response; raise Error if subscription was not successful
        try:
            msg_node = envelope.msg_node
            if msg_node.tag == wseTag('SubscribeResponse'):
                address = msg_node.xpath('wse:SubscriptionManager/wsa:Address/text()', namespaces=_global_nsmap)
                self.dev_reference_param = None

                reference_params = msg_node.xpath('wse:SubscriptionManager/wsa:ReferenceParameters',
                                                  namespaces=_global_nsmap)
                if reference_params:
                    self.dev_reference_param = reference_params[0]
                expires = msg_node.xpath('wse:Expires/text()', namespaces=_global_nsmap)

                self._subscription_manager_address = urllib.parse.urlparse(address[0])
                expire_seconds = isoduration.parse_duration(expires[0])
                self.expire_at = time.time() + expire_seconds
                self.is_subscribed = True
                self._logger.info('Subscribe was successful: expires at {}, address="{}"',
                                  self.expire_at, self._subscription_manager_address)
            else:
                # This is a failure response or even rubbish. log it and raise error
                self._logger.error('Subscribe response has unexpected content: {}', envelope.as_xml(pretty=True))
                self.is_subscribed = False
                raise SoapResponseException(envelope)
        except AttributeError:
            self._logger.error('Subscribe response has unexpected content: {}', envelope.as_xml(pretty=True))
            self.is_subscribed = False
            raise SoapResponseException(envelope)

    def subscribe(self, expire_minutes=60):
        self._logger.info('### startSubscription "{}" ###', self._filter)
        self.event_counter = 0
        self.expire_minutes = expire_minutes  # saved for later renewal, we will use the same interval
        # ToDo: check if there is more than one address. In that case a clever selection is needed
        address = self.dpws_hosted.endpoint_references[0].address
        envelope = self._mk_subscribe_envelope(address, expire_minutes)
        msg = 'subscribe {}'.format(self._filter)
        try:
            result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(self._device_epr, envelope,
                                                                                 msg=msg)
            self._handle_subscribe_response(result_envelope)
        except HTTPReturnCodeError:
            self._logger.error('could not subscribe: {}'.format(HTTPReturnCodeError))

    def _add_device_references(self, envelope):
        """ add references for requests to device (renew, getstatus, unsubscribe)"""
        if self.dev_reference_param is not None:
            for element in self.dev_reference_param:
                element_ = copy.copy(element)
                # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
                element_.set(wsaTag('IsReferenceParameter'), 'true')
                envelope.add_header_element(element_)

    def _mk_renew_envelope(self, expire_minutes):
        return self._msg_factory.mk_renew_envelope(
            urllib.parse.urlunparse(self._subscription_manager_address),
            dev_reference_param=self.dev_reference_param, expire_minutes=expire_minutes)

    def _handle_renew_response(self, envelope):
        # Check content of response; raise Error if subscription was not successful
        body_node = envelope.body_node
        renew_response = body_node.xpath('wse:RenewResponse', namespaces=_global_nsmap)
        if len(renew_response) == 1:
            # this means renew was accepted
            expires = body_node.xpath('wse:RenewResponse/wse:Expires/text()', namespaces=_global_nsmap)
            expire_seconds = isoduration.parse_duration(expires[0])
            self.expire_at = time.time() + expire_seconds
        else:
            raise SoapResponseException(envelope)

    def renew(self, expire_minutes=60):
        envelope = self._mk_renew_envelope(expire_minutes)
        try:
            result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(
                self._subscription_manager_address.path, envelope, msg='renew')
            self._logger.debug('{}', result_envelope.as_xml(pretty=True))
        except HTTPReturnCodeError as ex:
            self.is_subscribed = False
            self._logger.error('could not renew: {}'.format(HTTPReturnCodeError))
        except (http.client.HTTPException, ConnectionError) as ex:
            self._logger.warn('renew failed: {}', ex)
            self.is_subscribed = False
        except Exception as ex:
            self._logger.error('Exception in renew: {}', ex)
            self.is_subscribed = False
        else:
            try:
                self._handle_renew_response(result_envelope)
                return self.remaining_subscription_seconds
            except SoapResponseException as ex:
                self.is_subscribed = False
                self._logger.warn('renew failed: {}',
                                  etree_.tostring(ex.response_envelope.body_node, pretty_print=True))

    def unsubscribe(self):
        if not self.is_subscribed:
            return
        soap_envelope = self._msg_factory.mk_unsubscribe_envelope(
            urllib.parse.urlunparse(self._subscription_manager_address),
            dev_reference_param=self.dev_reference_param)

        result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(self._subscription_manager_address.path,
                                                                             soap_envelope, msg='unsubscribe')
        response_action = result_envelope.address.action
        # check response: response does not contain explicit status. If action== UnsubscribeResponse all is fine.
        if response_action == 'http://schemas.xmlsoap.org/ws/2004/08/eventing/UnsubscribeResponse':
            self._logger.info('unsubscribe: end of subscription {} was confirmed.', self._filter)
        else:
            self._logger.error('unsubscribe: unexpected response action: {}', result_envelope.as_xml(pretty=True))
            raise RuntimeError(
                'unsubscribe: unexpected response action: {}'.format(result_envelope.as_xml(pretty=True)))

    def _mk_get_status_envelope(self):
        return self._msg_factory.mk_getstatus_envelope(
            urllib.parse.urlunparse(self._subscription_manager_address),
            dev_reference_param=self.dev_reference_param)

    def get_status(self):
        """ Sends a GetStatus Request to the device.
        @return: the remaining time of the subscription or None, if the request was not successful
        """
        envelope = self._mk_get_status_envelope()
        try:
            result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(
                self._subscription_manager_address.path,
                envelope, msg='get_status')
        except HTTPReturnCodeError as ex:
            self.is_subscribed = False
            self._logger.error('could not get status: {}'.format(HTTPReturnCodeError))
        except (http.client.HTTPException, ConnectionError) as ex:
            self.is_subscribed = False
            self._logger.warn('get_status: Connection Error {} for subscription {}', ex, self._filter)
        except Exception as ex:
            self._logger.error('Exception in get_status: {}', ex)
            self.is_subscribed = False
        else:
            try:
                expires_node = result_envelope.msg_node.find('wse:Expires', namespaces=_global_nsmap)
                if expires_node is None:
                    self._logger.warn('get_status for {}: Could not find "Expires" node! get_status={} ', self._filter,
                                      result_envelope.rawdata)
                    raise SoapResponseException(result_envelope)
                expires = expires_node.text
                expires_value = isoduration.parse_duration(expires)
                self._logger.debug('get_status for {}: Expires = {} = {} seconds, counter = {}', self._filter,
                                   expires,
                                   expires_value,
                                   self.event_counter)
                return expires_value
            except AttributeError:
                self._logger.warn('No msg in envelope')

    def check_status(self, renew_limit):
        """ Calls get_status and updates internal data.
        @param renew_limit: a value in seconds. If remaining duration of subscription is less than this value, it renews the subscription.
        @return: None
        """
        if not self.is_subscribed:
            return

        remaining_time = self.get_status()
        if remaining_time is None:
            self.is_subscribed = False
            return
        if abs(remaining_time - self.remaining_subscription_seconds) > 10:
            self._logger.warn(
                'time delta between expected expire and reported expire  > 10 seconds. Will correct own expectation.')
            self.expire_at = time.time() + remaining_time

        if self.remaining_subscription_seconds < renew_limit:
            self._logger.info('renewing subscription')
            self.renew()

    def check_status_renew(self):
        """ Calls renew and updates internal data.
        @return: None
        """
        if self.is_subscribed:
            self.renew()

    @property
    def remaining_subscription_seconds(self):
        return self.expire_at - time.time()

    def on_notification(self, envelope):
        self.event_counter += 1
        self.notification = envelope

    @property
    def short_filter_string(self):
        return etc.short_filter_string(self._actions)

    def __str__(self):
        return 'Subscription of "{}", is_subscribed={}, remaining time = {} sec., count={}'.format(
            self.short_filter_string,
            self.is_subscribed,
            int(self.remaining_subscription_seconds),
            self.event_counter)


class SubscriptionClient(threading.Thread):
    """ Factory for Subscription objects, thread that automatically renews expiring subscriptions.
    @param notification_url: the destination url for notifications.
    @param end_to_url: if given the destination url for end subscription notifications; if not given, the notification_url is used.
    @param check_interval: the interval (in seconds ) for get_status requests. Defaults to SUBSCRIPTION_CHECK_INTERVAL
    @param ident: a string that is used in log output; defaults to empty string
     """
    all_subscriptions_okay = properties.ObservableProperty(True)  # a boolean
    keep_alive_with_renew = True  # enable as workaround if checkstatus is not supported

    def __init__(self, msg_factory, notification_url, end_to_url=None, check_interval=None, log_prefix=''):
        super().__init__(name='SubscriptionClient{}'.format(log_prefix))
        self.daemon = True
        self._msg_factory = msg_factory
        self._check_interval = check_interval or SUBSCRIPTION_CHECK_INTERVAL
        self.subscriptions = {}
        self._subscriptions_lock = threading.Lock()

        self._run = False
        self._notification_url = notification_url
        self._end_to_url = end_to_url or notification_url
        self._logger = loghelper.get_logger_adapter('sdc.client.subscrMgr', log_prefix)
        self.log_prefix = log_prefix

    def stop(self):
        self._run = False
        self.join(timeout=2)
        with self._subscriptions_lock:
            self.subscriptions.clear()

    def run(self):
        self._run = True
        try:
            while self._run:
                try:
                    for _ in range(self._check_interval):
                        time.sleep(1)
                        if not self._run:
                            return
                        # check if all subscriptions are okay
                        with self._subscriptions_lock:
                            not_okay = [s for s in self.subscriptions.values() if not s.is_subscribed]
                            self.all_subscriptions_okay = (len(not_okay) == 0)
                    with self._subscriptions_lock:
                        subscriptions = list(self.subscriptions.values())
                    for subscription in subscriptions:
                        if self.keep_alive_with_renew:
                            subscription.check_status_renew()
                        else:
                            subscription.check_status(renew_limit=self._check_interval * 5)
                    self._logger.debug('##### SubscriptionManager Interval ######')
                    for subscription in subscriptions:
                        self._logger.debug('{}', subscription)
                except Exception:
                    self._logger.error('##### check loop: {}', traceback.format_exc())
        finally:
            self._logger.info('terminating subscriptions check loop! self._run={}', self._run)

    def mk_subscription(self, dpws_hosted, filters):
        subscription = _ClSubscription(self._msg_factory, dpws_hosted, filters, self._notification_url,
                                       self._end_to_url,
                                       self.log_prefix)
        filter_ = ' '.join(filters)
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def on_subscription_end(self, envelope):
        subscr_ident_list = envelope.header_node.findall(_ClSubscription.IDENT_TAG, namespaces=_global_nsmap)
        statuus = envelope.body_node.xpath('wse:SubscriptionEnd/wse:Status/text()', namespaces=_global_nsmap)
        reasons = envelope.body_node.xpath('wse:SubscriptionEnd/wse:Reason/text()', namespaces=_global_nsmap)
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
            self._logger.warn('on_subscription_end: did not find any identifier in message')
            return
        subscr_ident = subscr_ident_list[0]
        for subscription in self.subscriptions.values():
            if subscr_ident.text == subscription.end_to_identifier.text:
                self._logger.info('on_subscription_end: received Subscription End for {} {}',
                                  subscription.short_filter_string,
                                  info)
                subscription.is_subscribed = False
                return
        self._logger.warn('on_subscription_end: have no subscription for identifier = {}', subscr_ident.text)

    def unsubscribe_all(self):
        with self._subscriptions_lock:
            current_subscriptions = list(self.subscriptions.values())  # make a copy
            self.subscriptions.clear()
            for subscription in current_subscriptions:
                try:
                    subscription.unsubscribe()
                except Exception:
                    self._logger.warn('unsubscribe error: {}\n call stack:{} ', traceback.format_exc(),
                                      traceback.format_stack())


class _DispatchError(Exception):
    def __init__(self, http_error_code, error_text):
        super().__init__()
        self.http_error_code = http_error_code
        self.error_text = error_text


class SOAPNotificationsDispatcher:
    """ receiver of all notifications"""

    def __init__(self, log_prefix, sdc_definitions):
        self._logger = loghelper.get_logger_adapter('sdc.client.notif_dispatch', log_prefix)
        self.log_prefix = log_prefix
        self._sdc_definitions = sdc_definitions
        self.methods = {}

    def register_function(self, action, func):
        self.methods[action] = func

    def dispatch(self, path, xml):
        start = time.time()
        normalized_xml = self._sdc_definitions.normalize_xml_text(xml)
        request = ReceivedSoap12Envelope(normalized_xml)
        try:
            action = request.address.action
        except AttributeError:
            raise _DispatchError(404, 'no action in request')
        self._logger.debug('received notification path={}, action = {}', path, action)

        try:
            func = self.methods[action]
        except KeyError:
            self._logger.error('action "{}" not registered. Known:{}'.format(action, self.methods.keys()))
            raise _DispatchError(404, 'action not registered')

        func(request)
        duration = time.time() - start
        if duration > 0.005:
            self._logger.debug('action {}: duration = {:.4f}sec', action, duration)
        return ''


class SOAPNotificationsDispatcherThreaded(SOAPNotificationsDispatcher):

    def __init__(self, ident, biceps_schema):
        super().__init__(ident, biceps_schema)
        self._queue = queue.Queue(1000)
        self._worker = threading.Thread(target=self._readqueue)
        self._worker.daemon = True
        self._worker.start()

    def dispatch(self, path, xml):
        normalized_xml = self._sdc_definitions.normalize_xml_text(xml)
        request = ReceivedSoap12Envelope(normalized_xml)
        try:
            action = request.address.action
        except AttributeError:
            raise _DispatchError(404, 'no action in request')
        self._logger.debug('received notification path={}, action = {}', path, action)

        try:
            func = self.methods[action]
        except KeyError:
            self._logger.error(
                'action "{}" not registered. Known:{}'.format(action, self.methods.keys()))
            raise _DispatchError(404, 'action not registered')
        self._queue.put((func, request, action))
        return ''

    def _readqueue(self):
        while True:
            func, request, action = self._queue.get()
            try:
                func(request)
            except Exception:
                self._logger.error(
                    'method {} for action "{}" failed:{}'.format(func.__name__, action, traceback.format_exc()))


class SOAPNotificationsHandler(HTTPRequestHandler):
    disable_nagle_algorithm = True
    wbufsize = 0xffff  # 64k buffer to prevent tiny packages
    RESPONSE_COMPRESS_MINSIZE = 256  # bytes, compress response it it is larger than this value (and other side supports compression)

    def do_POST(self):  # pylint: disable=invalid-name
        """SOAP POST gateway"""
        self.server.thread_obj.logger.debug('notification do_POST incoming')  # pylint: disable=protected-access
        dispatcher = self.server.dispatcher
        response_string = ''
        if dispatcher is None:
            # close this connection
            self.close_connection = True  # pylint: disable=attribute-defined-outside-init
            self.server.thread_obj.logger.warn(
                'received a POST request, but no dispatcher => returning 404 ')  # pylint:disable=protected-access
            self.send_response(404)  # not found
        else:
            request_bytes = self._read_request()

            self.server.thread_obj.logger.debug('notification {} bytes',
                                                request_bytes)  # pylint: disable=protected-access
            # execute the method
            commlog.get_communication_logger().log_soap_subscription_msg_in(request_bytes)
            try:
                response_string = self.server.dispatcher.dispatch(self.path, request_bytes)
                if response_string is None:
                    response_string = ''
                self.send_response(202, b'Accepted')
            except _DispatchError as ex:
                self.server.thread_obj.logger.error('received a POST request, but got _DispatchError => returning {}',
                                                    ex.http_error_code)  # pylint:disable=protected-access
                self.send_response(ex.http_error_code, ex.error_text)
            except Exception as ex:
                self.server.thread_obj.logger.error(
                    'received a POST request, but got Exception "{}"=> returning {}\n{}', ex, 500,
                    traceback.format_exc())  # pylint:disable=protected-access
                self.send_response(500, b'server error in dispatch')
        response_bytes = response_string.encode('utf-8')
        if len(response_bytes) > self.RESPONSE_COMPRESS_MINSIZE:
            response_bytes = self._compress_if_required(response_bytes)

        self.send_header("Content-Type", "application/soap+xml; charset=utf-8")
        self.send_header("Content-Length", len(response_bytes))  # this is necessary for correct keep-alive handling!
        self.end_headers()
        self.wfile.write(response_bytes)


class NotificationsReceiverDispatcherThread(threading.Thread):

    def __init__(self, my_ipaddress, ssl_context, log_prefix, sdc_definitions, supported_encodings,
                 soap_notifications_handler_class=None, async_dispatch=True):
        """

        :param my_ipaddress: http server will listen on this address
        :param ssl_context: http server uses this ssl context
        :param ident: used for logging
        :param sdc_definitions: namespaces etc
        :param supported_encodings: a list of strings
        :param soap_notifications_handler_class: if None, SOAPNotificationsHandler is used,
                otherwise the provided class ( a HTTPRequestHandler).
        :param async_dispatch: if True, incoming requests are queued and response is sent (processing is done later).
                                if False, response is sent after the complete processing is done.
        """
        super().__init__(
            name='Cl_NotificationsReceiver_{}'.format(log_prefix))
        self._ssl_context = ssl_context
        self._soap_notifications_handler_class = soap_notifications_handler_class
        self.daemon = True
        self.logger = loghelper.get_logger_adapter('sdc.client.notif_dispatch', log_prefix)

        self._my_ipaddress = my_ipaddress
        self.my_port = None
        self.base_url = None
        self.httpd = None
        self.supported_encodings = supported_encodings
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
            setattr(self.httpd, 'supported_encodings', self.supported_encodings)
            self.my_port = self.httpd.server_port
            self.logger.info('starting Notification receiver on {}:{}', self._my_ipaddress, self.my_port)
            if self._ssl_context:
                self.httpd.socket = self._ssl_context.wrap_socket(self.httpd.socket)
                self.base_url = 'https://{}:{}/'.format(self._my_ipaddress, self.my_port)
            else:
                self.base_url = 'http://{}:{}/'.format(self._my_ipaddress, self.my_port)
            self.httpd.dispatcher = self.dispatcher
            # make logger available for SOAPNotificationsHandler
            self.httpd.thread_obj = self  # pylint: disable=attribute-defined-outside-init
            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            self.logger.error(
                'Unhandled Exception at thread runtime. Thread will abort! {}'.format(traceback.format_exc()))
            raise

    def stop(self, close_all_connections=True):
        """
        @param close_all_connections: for testing purpose one might want to keep the connection handler threads alive.
                If param is False then they are kept alive.
        """
        self.httpd.shutdown()
        self.httpd.socket.close()
        if close_all_connections:
            if self.httpd.dispatcher is not None:
                self.httpd.dispatcher.methods = {}
                self.httpd.dispatcher = None  # this leads to a '503' reaction in SOAPNotificationsHandler
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    try:
                        request.shutdown(socket.SHUT_RDWR)
                        request.close()
                        self.logger.info('closed socket for notifications from {}', client_addr)
                    except OSError as ex:
                        # the connection is already closed
                        continue
                    except Exception as ex:
                        self.logger.warn('error closing socket for notifications from {}: {}', client_addr, ex)
            time.sleep(0.1)
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    thread.join(1)
                if thread.is_alive():
                    self.logger.warn('could not end client thread for notifications from {}', client_addr)
            del self.httpd.threads[:]
