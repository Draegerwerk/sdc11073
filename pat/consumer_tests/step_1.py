"""Tests Device Discovery."""

import functools
import logging
import threading
from collections.abc import Sequence

from sdc11073 import definitions_sdc, wsdiscovery

__STEP__ = '1'
logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def on_hello(step: str, event: threading.Event, expected_epr: str, addr_from: str, service: wsdiscovery.Service):
    """Handle hello events."""
    logger.debug('received hello from "%s" with epr "%s"', addr_from, service.epr, extra={'step': step})
    if service.epr == expected_epr:
        event.set()


def test_1a(discovery: wsdiscovery.WSDiscovery, epr: str) -> bool:
    """The Reference Provider sends Hello messages in ad-hoc mode."""
    step = f'{__STEP__}a'
    timeout = 10.0
    sent_hello_event = threading.Event()
    observer = functools.partial(on_hello, step, sent_hello_event, epr)
    try:
        discovery.set_remote_service_hello_callback(observer)
        if sent_hello_event.wait(timeout):
            logger.info('Hello received in ad-hoc mode', extra={'step': step})
            return True
        logger.error('Hello not received in ad-hoc mode within %ss.', timeout, extra={'step': step})
        return False
    finally:
        discovery.set_remote_service_hello_callback(None)


def on_probe_matches(step: str, event: threading.Event, expected_epr: str, services: Sequence[wsdiscovery.Service]):
    """Handle probe matches events."""
    logger.debug('received probe match with services %s', services, extra={'step': step})
    if any(service for service in services if service.epr == expected_epr):
        event.set()


def on_resolve_match(step: str, event: threading.Event, expected_epr: str, service: wsdiscovery.Service):
    """Handle resolve match events."""
    logger.debug('received resolve match from epr "%s"', service.epr, extra={'step': step})
    if service.epr == expected_epr:
        event.set()


def test_1b(discovery: wsdiscovery.WSDiscovery, epr: str) -> bool:
    """The Reference Provider answers to Probe and Resolve messages in ad-hoc mode."""
    step = f'{__STEP__}b'
    timeout = 10.0

    # if the epr is already known you can directly use resolve, but test specification requires probe to be tested
    probe_matches_event = threading.Event()
    observer = functools.partial(on_probe_matches, step, probe_matches_event, epr)
    try:
        discovery.set_on_probe_matches_callback(observer)
        discovery._send_probe(types=definitions_sdc.SdcV1Definitions.MedicalDeviceTypesFilter)  # noqa: SLF001
        result = probe_matches_event.wait(timeout)
    finally:
        discovery.set_on_probe_matches_callback(None)
    if result:
        logger.info('Probe matches received in ad-hoc mode', extra={'step': step})
    else:
        logger.error('Probe matches not received in ad-hoc mode within %ss.', timeout, extra={'step': step})
    if not result:
        return False

    resolve_match_event = threading.Event()
    observer = functools.partial(on_resolve_match, step, resolve_match_event, epr)
    try:
        discovery.set_remote_service_resolve_match_callback(observer)
        discovery._send_resolve(epr)  # noqa: SLF001
        result = resolve_match_event.wait(timeout)
    finally:
        discovery.set_remote_service_resolve_match_callback(None)
    if result:
        logger.info('Resolve match received in ad-hoc mode', extra={'step': step})
    else:
        logger.error('Resolve match not received in ad-hoc mode within %ss.', timeout, extra={'step': step})
    return result


def test_1c():
    """The Reference Provider sends Hello messages in managed mode by using the Discovery Proxy."""
    raise NotImplementedError


def test_1d():
    """The Reference Provider answers to Probe and Resolve messages in managed mode by using the Discovery Proxy."""
    raise NotImplementedError
