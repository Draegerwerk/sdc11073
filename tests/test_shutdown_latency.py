"""Tests that retransmit gaps and stop() calls are not slowed down by long fixed sleeps."""

from __future__ import annotations

import itertools
import logging
import queue
import threading
import time
from unittest import mock

from sdc11073.consumer.subscription import ConsumerSubscriptionManager
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase
from sdc11073.provider.subscriptionmgr_base import SubscriptionsManagerBase
from sdc11073.pysoap.msgfactory import MessageFactory
from sdc11073.pysoap.msgreader import MessageReader
from sdc11073.wsdiscovery import networkingthread

# generous upper bound for stop() calls, the former implementations needed up to 1 s (or 0.5 s for the http server)
MAX_STOP_DURATION = 0.3


def _enqueued_send_times(repeat_params: networkingthread._UdpRepeatParams) -> list[float]:
    """Enqueue one message and return the scheduled send times, without creating any sockets."""
    thread = networkingthread.NetworkingThread.__new__(networkingthread.NetworkingThread)
    thread._quit_send_event = threading.Event()
    thread._send_queue = queue.PriorityQueue()
    thread._logger = logging.getLogger('test')
    # use the largest possible first gap, so that doubling hits the upper limit as early as possible
    with mock.patch.object(networkingthread.random, 'randrange', return_value=repeat_params.max_delay_ms):
        thread._repeated_enqueue_msg(mock.MagicMock(), repeat_params)
    send_times = []
    while not thread._send_queue.empty():
        send_times.append(thread._send_queue.get().send_time)
    return send_times


def test_multicast_repeat_gap_is_limited_by_upper_delay():
    params = networkingthread.MULTICAST_REPEAT_PARAMS
    send_times = _enqueued_send_times(params)
    assert len(send_times) == params.repeat + 1
    gaps = [b - a for a, b in itertools.pairwise(send_times)]
    assert gaps[0] == params.max_delay_ms / 1000.0
    for gap in gaps:
        assert gap <= params.upper_delay_ms / 1000.0 + 1e-9


def test_provider_subscription_manager_stops_fast():
    sdc = SdcV1Definitions
    msg_factory = MessageFactory(sdc, None, logger=None, validate=False)
    mgr = SubscriptionsManagerBase(sdc, msg_factory, mock.MagicMock(), max_subscription_duration=30)
    time.sleep(0.1)  # let housekeeping thread enter its wait
    start = time.monotonic()
    mgr.stop_all(send_subscription_end=False)
    assert time.monotonic() - start < MAX_STOP_DURATION


def test_consumer_subscription_manager_stops_fast():
    sdc = SdcV1Definitions
    msg_factory = MessageFactory(sdc, None, logger=None, validate=False)
    msg_reader = MessageReader(sdc, None, logger=None, validate=False)
    for fixed_renew_interval in (None, 10):
        mgr = ConsumerSubscriptionManager(
            msg_reader,
            msg_factory,
            sdc.data_model,
            mock.MagicMock(),
            notification_url='http://localhost:8080/notify',
            fixed_renew_interval=fixed_renew_interval,
        )
        mgr.start()
        time.sleep(0.1)  # let thread enter its wait
        start = time.monotonic()
        mgr.stop()
        assert time.monotonic() - start < MAX_STOP_DURATION
        assert not mgr.is_alive()


def test_http_server_stops_fast():
    server = HttpServerThreadBase('127.0.0.1', None, [], logging.getLogger('test'))
    server.start()
    assert server.started_evt.wait(5)
    time.sleep(0.1)  # let serve_forever enter its poll loop
    start = time.monotonic()
    server.stop()
    assert time.monotonic() - start < MAX_STOP_DURATION
    server.join(timeout=5)
