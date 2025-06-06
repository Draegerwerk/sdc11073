"""Tests Device Discovery."""

import functools
import logging
import threading
from collections.abc import Sequence

from pat.ReferenceTestV2.consumer import result_collector
from sdc11073 import definitions_sdc, wsdiscovery

__STEP__ = '1'
logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def on_hello(event: threading.Event, expected_epr: str, addr_from: str, service: wsdiscovery.Service):
    # TODO: how to ensure that this hello was received in ad-hoc mode and not from a discovery proxy?
    logger.debug('received hello from "%s" with epr "%s"', addr_from, service.epr)
    if service.epr == expected_epr:
        event.set()


def test_1a(discovery: wsdiscovery.WSDiscovery, epr: str):
    """The Reference Provider sends Hello messages in ad-hoc mode."""
    step = f'{__STEP__}a'
    timeout = 10.0
    sent_hello_event = threading.Event()
    observer = functools.partial(on_hello, sent_hello_event, epr)
    discovery.set_remote_service_hello_callback(observer)
    if sent_hello_event.wait(timeout):
        result_collector.ResultCollector.log_success(step=step, message='Hello received in ad-hoc mode')
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Hello not received in ad-hoc mode within {timeout}s.',
        )
    discovery.set_remote_service_hello_callback(None)


def on_probe_matches(event: threading.Event, expected_epr: str, services: Sequence[wsdiscovery.Service]):
    logger.debug('received probe match with services %s', services)
    if any(service for service in services if service.epr == expected_epr):
        event.set()


def on_resolve_match(event: threading.Event, expected_epr: str, service: wsdiscovery.Service):
    logger.debug('received resolve match from epr "%s"', service.epr)
    if service.epr == expected_epr:
        event.set()


def test_1b(discovery: wsdiscovery.WSDiscovery, epr: str):
    """The Reference Provider answers to Probe and Resolve messages in ad-hoc mode."""
    step = f'{__STEP__}b'
    timeout = 10.0

    # if the epr is already known you can directly use resolve, but test specification requires probe to be tested
    probe_matches_event = threading.Event()
    observer = functools.partial(on_probe_matches, probe_matches_event, epr)
    discovery.set_on_probe_matches_callback(observer)
    discovery._send_probe(types=definitions_sdc.SdcV1Definitions.MedicalDeviceTypesFilter)
    if probe_matches_event.wait(timeout):
        result_collector.ResultCollector.log_success(step=step, message='Probe matches received in ad-hoc mode')
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Probe matches not received in ad-hoc mode within {timeout}s.',
        )
    discovery.set_on_probe_matches_callback(None)

    resolve_match_event = threading.Event()
    observer = functools.partial(on_resolve_match, resolve_match_event, epr)
    discovery.set_remote_service_resolve_match_callback(observer)
    discovery._send_resolve(epr)
    if resolve_match_event.wait(timeout):
        result_collector.ResultCollector.log_success(step=step, message='Resolve match received in ad-hoc mode')
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Resolve match not received in ad-hoc mode within {timeout}s.',
        )
    discovery.set_remote_service_resolve_match_callback(None)


def test_1c():
    """The Reference Provider sends Hello messages in managed mode by using the Discovery Proxy."""
    raise NotImplementedError


def test_1d():
    """The Reference Provider answers to Probe and Resolve messages in managed mode by using the Discovery Proxy."""
    raise NotImplementedError
