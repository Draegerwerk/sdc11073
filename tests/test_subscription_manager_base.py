"""Tests for subscription manager base class."""

from __future__ import annotations

import http.client
import socket
from types import SimpleNamespace

import pytest
from lxml import etree

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.dispatch.request import RequestData
from sdc11073.namespaces import EventingActions
from sdc11073.provider.subscriptionmgr_base import (
    ActionBasedSubscription,
    RoundTripData,
    SubscriptionsManagerBase,
    _mk_dispatch_identifier,
)
from sdc11073.pysoap.msgfactory import MessageFactory
from sdc11073.pysoap.msgreader import MessageReader
from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.xml_types import eventing_types as evt
from sdc11073.xml_types.addressing_types import HeaderInformationBlock


class DummySoapClientPool:
    def get_soap_client(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
        return SimpleNamespace(post_message_to=lambda *_, **__: None)

    def forget_usr(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        # No-op stub for tests
        return None


class TestSubscriptionsManager(SubscriptionsManagerBase):
    def __init__(self, sdc_definitions, msg_factory, soap_client_pool):  # noqa: ANN001
        super().__init__(sdc_definitions, msg_factory, soap_client_pool, max_subscription_duration=30, log_prefix='t')

    def _mk_subscription_instance(self, request_data: RequestData) -> ActionBasedSubscription:
        subscribe = evt.Subscribe.from_node(request_data.message_data.p_msg.msg_node)
        subscription = ActionBasedSubscription(
            self,
            subscribe,
            accepted_encodings=[],
            base_urls=self.base_urls,
            max_subscription_duration=30,
            soap_client_pool=self._soap_client_pool,
            msg_factory=self._msg_factory,
            log_prefix='t',
        )
        # identify via reference param only (path based dispatch not used here)
        subscription.set_reference_parameter()
        return subscription


def _mk_received(subscribe_msg: bytes, msg_reader: MessageReader) -> RequestData:
    rd = RequestData(
        {},
        '/uuid',
        'peer',
        subscribe_msg,
        msg_reader.read_received_message(subscribe_msg, validate=False),
    )
    # simulate provider dispatcher consuming the first path segment (uuid)
    rd.consume_current_path_element()
    return rd


def test_roundtrip_data_repr():
    values = [0.1, 0.2, 0.3]
    r = RoundTripData(values, abs_max=1)
    assert r.min == min(values)
    assert r.max == max(values)
    assert r.avg == sum(values) / len(values)
    s = repr(r)
    assert 'min=' in s
    assert 'max=' in s
    assert 'avg=' in s
    assert 'absmax=' in s


def test_mk_dispatch_identifier():
    ident = etree.Element('{ns}Id')
    ident.text = 'abc'
    assert _mk_dispatch_identifier([ident], 'foo/bar') == ('abc', 'foo/bar')
    assert _mk_dispatch_identifier([ident], '') == ('abc', None)


def test_action_based_subscription_match_and_short_names():
    subscribe = evt.Subscribe()
    subscribe.set_filter('http://x/y/Act1 http://x/y/Act2')
    subscribe.Delivery.NotifyTo.Address = 'http://localhost:8000/notify'
    sub = ActionBasedSubscription(
        mgr=None,
        subscribe_request=subscribe,
        accepted_encodings=[],
        base_urls=[],
        max_subscription_duration=60,
        soap_client_pool=DummySoapClientPool(),
        msg_factory=None,
        log_prefix='t',
    )
    assert sub.matches('Act1')
    assert sub.matches('http://x/y/Act2')
    assert not sub.matches('Other')
    assert sub.short_filter_names() == ['Act1', 'Act2']


def test_subscribe_renew_get_status_and_unsubscribe_flow():
    sdc = SdcV1Definitions
    msg_factory = MessageFactory(sdc, None, logger=None, validate=False)
    msg_reader = MessageReader(sdc, None, logger=None, validate=False)
    mgr = TestSubscriptionsManager(sdc, msg_factory, DummySoapClientPool())
    mgr.set_base_urls([SimpleNamespace(scheme='http', netloc='127.0.0.1:9000')])

    # build a Subscribe SOAP request
    subscribe = evt.Subscribe()
    subscribe.set_filter('http://x/y/Act')
    subscribe.Delivery.NotifyTo.Address = 'http://127.0.0.1:9999/notify'
    subscribe.Expires = 25
    created = msg_factory.mk_soap_message(HeaderInformationBlock(action=subscribe.action, addr_to='n/a'), subscribe)
    rd = _mk_received(created.serialize(validate=False), msg_reader)

    # subscribe
    response = mgr.on_subscribe_request(rd)
    resp_rd = msg_reader.read_received_message(response.serialize(validate=False), validate=False)
    body = evt.SubscribeResponse.from_node(resp_rd.p_msg.msg_node)
    assert body.Expires <= 25  # remaining seconds is used to calculate the Expires

    # prepare GetStatus request with same identifier
    ident = next(iter(mgr._subscriptions.objects)).reference_parameters[0]
    gs = evt.GetStatus()
    hib = HeaderInformationBlock(action=gs.action, addr_to='n/a', reference_parameters=[ident])
    gs_msg = msg_factory.mk_soap_message(hib, gs)
    rd_gs = _mk_received(gs_msg.serialize(validate=False), msg_reader)
    # get status
    gs_resp = mgr.on_get_status_request(rd_gs)
    gs_rd = msg_reader.read_received_message(gs_resp.serialize(validate=False), validate=False)
    gs_body = evt.GetStatusResponse.from_node(gs_rd.p_msg.msg_node)
    assert gs_body.Expires > 0
    assert gs_body.Expires <= 25

    # renew with too large expires -> capped by manager max (30)
    rn = evt.Renew()
    rn.Expires = 1000
    rn_msg = msg_factory.mk_soap_message(hib, rn)
    rd_rn = _mk_received(rn_msg.serialize(validate=False), msg_reader)
    rn_resp = mgr.on_renew_request(rd_rn)
    rn_rd = msg_reader.read_received_message(rn_resp.serialize(validate=False), validate=False)
    rn_body = evt.RenewResponse.from_node(rn_rd.p_msg.msg_node)
    assert rn_body.Expires <= 30  # remaining seconds is used to calculate the Expires

    # unsubscribe success path
    unsub = evt.Unsubscribe()
    unsub_msg = msg_factory.mk_soap_message(hib, unsub)
    rd_un = _mk_received(unsub_msg.serialize(validate=False), msg_reader)
    un_resp = mgr.on_unsubscribe_request(rd_un)
    un_rd = msg_reader.read_received_message(un_resp.serialize(validate=False), validate=False)
    assert un_rd.action == EventingActions.UnsubscribeResponse

    mgr.stop_all(send_subscription_end=False)


def test_send_notification_report_error_handling():
    # manager with no schema validation
    sdc = SdcV1Definitions
    msg_factory = MessageFactory(sdc, None, logger=None, validate=False)
    mgr = TestSubscriptionsManager(sdc, msg_factory, DummySoapClientPool())

    class DummySub:
        notify_to_address = 'http://host/notify'

        def __init__(self, exc: Exception | None):
            self._exc = exc

        def send_notification_report(self, *_):  # noqa: ANN002
            if self._exc:
                raise self._exc

    # errors that shall be swallowed
    for exc in (
        ConnectionRefusedError('x'),
        HTTPReturnCodeError(500, 'err', None),
        http.client.NotConnected(),
        socket.timeout(),  # noqa: UP041
    ):
        mgr._send_notification_report(DummySub(exc), etree.Element('n'), 'act')

    # DocumentInvalid -> re-raised
    with pytest.raises(etree.DocumentInvalid):
        mgr._send_notification_report(DummySub(etree.DocumentInvalid('bad')), etree.Element('n'), 'act')
