"""Tests for consumer subscription and consumer subscription manager."""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest
from lxml import etree

from sdc11073.consumer.subscription import (
    ClientSubscriptionManagerReferenceParams,
    ConsumerSubscription,
    ConsumerSubscriptionManager,
)
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.dispatch.request import RequestData
from sdc11073.namespaces import EventingActions
from sdc11073.pysoap.msgfactory import CreatedMessage, MessageFactory
from sdc11073.pysoap.msgreader import MessageReader
from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.xml_types import eventing_types as evt
from sdc11073.xml_types.addressing_types import HeaderInformationBlock


def test_consumer_subscription_short_filter_string_and_notification():
    sdc = SdcV1Definitions
    msg_factory = MessageFactory(sdc, None, logger=None, validate=False)
    data_model = sdc.data_model

    # hosted service stub with endpoint
    hosted = SimpleNamespace(EndpointReference=[SimpleNamespace(Address='http://host:9000/svc')])

    filter_type = evt.FilterType()
    filter_type.text = 'http://a/b/Action1 http://a/b/ActionTwo'
    subscription = ConsumerSubscription(
        msg_factory,
        data_model,
        mock.MagicMock(),
        hosted,
        filter_type,
        notification_url='http://localhost:8080/notify',
        end_to_url='http://localhost:8080/end',
        log_prefix='t',
    )

    assert subscription.short_filter_string == 'Action1, ActionTwo'

    # on_notification increments counter and stores message
    message_data = SimpleNamespace(action='Act', mdib_version_group='V', p_msg=SimpleNamespace(raw='x'))
    request_data = SimpleNamespace(message_data=message_data)
    subscription.on_notification(request_data)
    assert subscription.event_counter == 1
    assert subscription.notification_data is message_data
    assert subscription.notification_msg is message_data.p_msg


def test_consumer_subscription_manager_paths_and_end():
    sdc = SdcV1Definitions
    msg_factory = MessageFactory(sdc, None, logger=None, validate=False)
    msg_reader = MessageReader(sdc, None, logger=None, validate=False)
    data_model = sdc.data_model

    mgr = ConsumerSubscriptionManager(
        msg_reader,
        msg_factory,
        data_model,
        mock.MagicMock(),
        notification_url='http://localhost:8080/notify',
        end_to_url='http://localhost:8080/end',
        fixed_renew_interval=None,
        log_prefix='t',
    )

    hosted = SimpleNamespace(EndpointReference=[SimpleNamespace(Address='http://host:9000/svc')])
    filter_type = evt.FilterType()
    filter_type.text = 'http://a/b/Action1'
    sub = mgr.mk_subscription(hosted, filter_type)
    assert sub.notification_url.endswith('/subscr1')
    assert sub.end_to_url.endswith('/subscr1_e')

    # simulate provider sending SubscriptionEnd to our unique end_to path
    sub.is_subscribed = True
    end = evt.SubscriptionEnd()
    end.Status = 'DeviceShutdown'
    end.add_reason('bye', 'en-US')
    node = end.as_etree_node(end.NODETYPE, sdc.data_model.ns_helper.partial_map(sdc.data_model.ns_helper.WSE))
    message_data = SimpleNamespace(p_msg=SimpleNamespace(msg_node=node))

    rd = RequestData({}, '/' + sub.end_to_url.rsplit('/', 1)[-1], 'peer', b'', message_data)
    handled = mgr.on_subscription_end(rd)
    assert handled is sub
    assert sub.is_subscribed is False
    assert sub.end_status == 'DeviceShutdown'

    end_reason = evt.LanguageSpecificStringType()
    end_reason.text = 'bye'
    end_reason.lang = 'en-US'
    assert sub.end_reason == end_reason


class FakeSoapClient:
    def __init__(self, msg_factory: MessageFactory, msg_reader: MessageReader):
        self._mf = msg_factory
        self._mr = msg_reader
        self.subscribe_expires = 10
        self.renew_expires = 20
        self.status_expires = 7
        self.unsubscribe_ok = True
        self.renew_raise_http_error = False
        self.get_status_raise_http_error = False
        self.last_subscribe_payload = None

    def post_message_to(self, path: str, message: CreatedMessage, msg: str = ''):  # noqa: ARG002, ANN201
        action = message.p_msg.header_info_block.Action
        if action == EventingActions.Subscribe:
            # capture payload to verify extra elements/attributes
            self.last_subscribe_payload = message.p_msg.payload_element
            payload = evt.SubscribeResponse()
            payload.Expires = self.subscribe_expires
            payload.SubscriptionManager.Address = 'http://host:9000/manager'
            resp = self._mf.mk_soap_message(HeaderInformationBlock(action=payload.action, addr_to='n/a'), payload)
            return self._mr.read_received_message(resp.serialize(validate=False), validate=False)
        if action == EventingActions.Renew:
            if self.renew_raise_http_error:
                raise HTTPReturnCodeError(400, 'Bad', None)
            payload = evt.RenewResponse()
            payload.Expires = self.renew_expires
            resp = self._mf.mk_soap_message(HeaderInformationBlock(action=payload.action, addr_to='n/a'), payload)
            return self._mr.read_received_message(resp.serialize(validate=False), validate=False)
        if action == EventingActions.GetStatus:
            if self.get_status_raise_http_error:
                raise HTTPReturnCodeError(400, 'Bad', None)
            payload = evt.GetStatusResponse()
            payload.Expires = self.status_expires
            resp = self._mf.mk_soap_message(HeaderInformationBlock(action=payload.action, addr_to='n/a'), payload)
            return self._mr.read_received_message(resp.serialize(validate=False), validate=False)
        if action == EventingActions.Unsubscribe:
            if self.unsubscribe_ok:
                payload = evt.UnsubscribeResponse()
                resp = self._mf.mk_soap_message(HeaderInformationBlock(action=payload.action, addr_to='n/a'), payload)
                return self._mr.read_received_message(resp.serialize(validate=False), validate=False)
            # wrong action on purpose
            payload = evt.SubscribeResponse()
            payload.Expires = 1
            payload.SubscriptionManager.Address = 'http://host:9000/manager'
            resp = self._mf.mk_soap_message(HeaderInformationBlock(action=payload.action, addr_to='n/a'), payload)
            return self._mr.read_received_message(resp.serialize(validate=False), validate=False)
        msg = f'unexpected action: {action}'
        raise AssertionError(msg)


def _make_get_soap_client(fake: FakeSoapClient):  # noqa: ANN202
    return lambda _addr: fake


def _hosted():  # noqa: ANN202
    return SimpleNamespace(EndpointReference=[SimpleNamespace(Address='http://host:9000/svc')])


def _filter_type(text: str) -> evt.FilterType:
    ft = evt.FilterType()
    ft.text = text
    return ft


def test_subscribe_renew_get_status_unsubscribe_and_str_and_remaining():
    sdc = SdcV1Definitions
    mf = MessageFactory(sdc, None, logger=None, validate=False)
    mr = MessageReader(sdc, None, logger=None, validate=False)
    fake = FakeSoapClient(mf, mr)

    sub = ConsumerSubscription(
        mf,
        sdc.data_model,
        _make_get_soap_client(fake),
        _hosted(),
        _filter_type('http://a/b/Action1'),
        notification_url='http://localhost:8080/notify',
        end_to_url='http://localhost:8080/end',
        log_prefix='t',
    )
    # subscribe with extra elements/attributes
    extra = etree.Element('Extra')
    sub.subscribe(expires=5, any_elements=[extra], any_attributes={'foo': 'bar'})
    assert sub.is_subscribed is True
    assert sub._subscription_manager_path == '/manager'
    assert sub.granted_expires == fake.subscribe_expires
    assert sub.remaining_subscription_seconds > 0
    # verify extras made it into the outgoing payload
    payload = fake.last_subscribe_payload
    assert any(child.tag.endswith('Extra') for child in payload)
    assert payload.attrib.get('foo') == 'bar'

    # renew success
    got = sub.renew(fake.renew_expires + 1)
    assert got == fake.renew_expires
    # get_status success
    got_status = sub.get_status()
    assert got_status == fake.status_expires

    # unsubscribe success
    sub.unsubscribe()
    assert sub.is_subscribed is False

    # __str__ contains summary
    s = str(sub)
    assert 'Subscription of' in s
    assert 'Action1' in s


def test_subscribe_without_endto_and_error_paths():
    sdc = SdcV1Definitions
    mf = MessageFactory(sdc, None, logger=None, validate=False)
    mr = MessageReader(sdc, None, logger=None, validate=False)
    fake = FakeSoapClient(mf, mr)

    # end_to_url None path
    sub = ConsumerSubscription(
        mf,
        sdc.data_model,
        _make_get_soap_client(fake),
        _hosted(),
        _filter_type('http://a/b/Act'),
        notification_url='http://localhost:8080/notify',
        end_to_url=None,
        log_prefix='t',
    )
    sub.subscribe(expires=5)
    assert sub.is_subscribed is True

    # renew http error => is_subscribed False
    fake.renew_raise_http_error = True
    assert sub.renew(30) == 0.0
    assert sub.is_subscribed is False

    # re-subscribe to test get_status http error
    fake.renew_raise_http_error = False
    sub.subscribe(expires=5)
    fake.get_status_raise_http_error = True
    assert sub.get_status() == 0.0
    assert sub.is_subscribed is False

    # re-subscribe to test unsubscribe unexpected action
    sub.subscribe(expires=5)
    fake.unsubscribe_ok = False
    with pytest.raises(ValueError, match='unsubscribe: unexpected response action'):
        sub.unsubscribe()


def test_client_subscription_manager_reference_params_find_and_unsubscribe_all():
    sdc = SdcV1Definitions
    mf = MessageFactory(sdc, None, logger=None, validate=False)
    mr = MessageReader(sdc, None, logger=None, validate=False)
    fake = FakeSoapClient(mf, mr)
    mgr = ConsumerSubscriptionManager(
        mr,
        mf,
        sdc.data_model,
        _make_get_soap_client(fake),
        notification_url='http://localhost:8080/notify',
        end_to_url='http://localhost:8080/end',
        fixed_renew_interval=None,
        log_prefix='t',
    )

    hosted = _hosted()
    ft = _filter_type('http://a/b/Action')
    sub = mgr.mk_subscription(hosted, ft)

    # simulate provider calling unsubscribe_all across multiple subscriptions
    # patch subscription to raise HTTPException-like (using HTTPReturnCodeError) and then generic Exception
    class BadSub1(ConsumerSubscription):
        def unsubscribe(self):
            raise HTTPReturnCodeError(400, 'err', None)

    class BadSub2(ConsumerSubscription):
        def unsubscribe(self):
            raise RuntimeError('boom')

    with mgr._subscriptions_lock:
        mgr.subscriptions = {
            'ok': sub,
            'bad1': BadSub1(mf, sdc.data_model, _make_get_soap_client(fake), hosted, ft, 'n', 'e', 't'),
            'bad2': BadSub2(mf, sdc.data_model, _make_get_soap_client(fake), hosted, ft, 'n', 'e', 't'),
        }
    assert mgr.unsubscribe_all() is False

    # ClientSubscriptionManagerReferenceParams lookup by reference parameter
    mgr_ref = ClientSubscriptionManagerReferenceParams(
        mr,
        mf,
        sdc.data_model,
        _make_get_soap_client(fake),
        notification_url='http://localhost:8080/notify',
        end_to_url='http://localhost:8080/end',
        fixed_renew_interval=None,
        log_prefix='t',
    )
    sub_ref = mgr_ref.mk_subscription(hosted, ft)
    # fake request_data with matching reference parameter
    msg = SimpleNamespace(
        p_msg=SimpleNamespace(
            header_info_block=SimpleNamespace(
                reference_parameters=[sub_ref.end_to_identifier],
            ),
        ),
    )

    rd = RequestData({}, '/', 'peer', b'', msg)
    assert mgr_ref._find_subscription(rd, 'x') is sub_ref
