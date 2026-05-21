"""Implementation of reference consumer v2."""

from __future__ import annotations

import logging
import pathlib
import sys
from concurrent import futures
from typing import TYPE_CHECKING

from pat import common
from pat.consumer_tests import step_1, step_2, step_3, step_4, step_5, step_6, step_7
from sdc11073.consumer.consumerimpl import SdcConsumer
from sdc11073.wsdiscovery import WSDiscovery

if TYPE_CHECKING:
    import sdc11073.certloader


def _setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(step)s - %(message)s'))
    logger = logging.getLogger('pat.consumer')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    sdc_handler = logging.StreamHandler(sys.stdout)
    sdc_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    sdc_logger = logging.getLogger('sdc.client.mdib')
    sdc_logger.addHandler(sdc_handler)
    sdc_logger.setLevel(logging.DEBUG)


def run_ref_test(  # noqa: PLR0913, PLR0915
    ip: str,
    epr: str,
    certificate_folder: pathlib.Path | None,
    certificate_password: str | None,
    execute_1a: bool,
    network_delay: float,
    timeout_ref_provider: float = 10.0,
) -> bool:
    """Run reference test."""
    _setup_logging()

    ssl_context_container: sdc11073.certloader.SSLContextContainer | None = None
    if certificate_folder:
        ssl_context_container = common.get_ssl_context(certificate_folder, certificate_password)

    with WSDiscovery(ip) as wsd:
        res_1a = step_1.test_1a(wsd, epr, timeout=timeout_ref_provider) if execute_1a else None
        res_1b = step_1.test_1b(wsd, epr, timeout=timeout_ref_provider)
        if not res_1b:
            return False
        services = wsd.get_found_remote_services()  # services have already been found in 1b

    service = next(s for s in services if s.epr == epr)
    consumer = SdcConsumer.from_wsd_service(service, ssl_context_container=ssl_context_container, validate=True)
    res_2a = step_2.test_2a(consumer)
    try:
        res_2b, mdib = step_2.test_2b(consumer)
        if not res_2b:
            return False

        res_3a = step_3.test_3a(consumer)
        res_3b = step_3.test_3b(consumer)

        with futures.ThreadPoolExecutor() as pool:
            thread_test_4a = pool.submit(step_4.test_4a, mdib)
            thread_test_4b = pool.submit(step_4.test_4b, mdib)
            thread_test_4c = pool.submit(step_4.test_4c, mdib)
            thread_test_4d = pool.submit(step_4.test_4d, mdib)
            thread_test_4e = pool.submit(step_4.test_4e, mdib)
            thread_test_4f = pool.submit(step_4.test_4f, mdib, network_delay)
            thread_test_4g = pool.submit(step_4.test_4g, mdib)
            thread_test_4h = pool.submit(step_4.test_4h, mdib)
            thread_test_4i = pool.submit(step_4.test_4i, mdib, network_delay)
            thread_test_5a = pool.submit(step_5.test_5a, mdib)
            thread_test_5b = pool.submit(step_5.test_5b, mdib)
            thread_test_6b = pool.submit(step_6.test_6b, consumer)
            thread_test_6c = pool.submit(step_6.test_6c, consumer)
            thread_test_6d = pool.submit(step_6.test_6d, consumer)
            thread_test_6e = pool.submit(step_6.test_6e, consumer)
            thread_test_6f = pool.submit(step_6.test_6f, consumer)
            thread_test_7a = pool.submit(step_7.test_7a, consumer.localization_service_client)

            test_4a = thread_test_4a.result()
            test_4b = thread_test_4b.result()
            test_4c = thread_test_4c.result()
            test_4d = thread_test_4d.result()
            test_4e = thread_test_4e.result()
            test_4f = thread_test_4f.result()
            test_4g = thread_test_4g.result()
            test_4h = thread_test_4h.result()
            test_4i = thread_test_4i.result()
            test_5a = thread_test_5a.result()
            test_5b = thread_test_5b.result()
            test_6b = thread_test_6b.result()
            test_6c = thread_test_6c.result()
            test_6d = thread_test_6d.result()
            test_6e = thread_test_6e.result()
            test_6f = thread_test_6f.result()
            test_7a = thread_test_7a.result()

            test_7b = step_7.test_7b(consumer.localization_service_client) if test_7a else False
            test_7c = step_7.test_7c(consumer.localization_service_client) if test_7a else False
    finally:
        consumer.stop_all()

    if execute_1a:
        print('1a:', res_1a)
    print('1b:', res_1b)
    print('2a:', res_2a)
    print('2b:', res_2b)
    print('3a:', res_3a)
    print('3b:', res_3b)
    print('4a:', test_4a)
    print('4b:', test_4b)
    print('4c:', test_4c)
    print('4d:', test_4d)
    print('4e:', test_4e)
    print('4f:', test_4f)
    print('4g:', test_4g)
    print('4h:', test_4h)
    print('4i:', test_4i)
    print('5a:', test_5a)
    print('5b:', test_5b)
    print('6b:', test_6b)
    print('6c:', test_6c)
    print('6d:', test_6d)
    print('6e:', test_6e)
    print('6f:', test_6f)
    print('7a:', test_7a)
    print('7b:', test_7b)
    print('7c:', test_7c)

    results = [
        res_1b,
        res_2a,
        res_2b,
        res_3a,
        res_3b,
        test_4a,
        test_4b,
        test_4c,
        test_4d,
        test_4e,
        test_4f,
        test_4g,
        test_4h,
        test_4i,
        test_5a,
        test_5b,
        test_6b,
        test_6c,
        test_6d,
        test_6e,
        test_6f,
        test_7a,
        test_7b,
        test_7c,
    ]
    if execute_1a:
        results.append(res_1a)
    return any(results) and all(results)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='run plug-a-thon test consumer')
    parser.add_argument('--ip', required=True, help='Network adapter IP address to use.')
    parser.add_argument('--epr', required=True, help='Explicit endpoint reference to search for.')
    parser.add_argument('--certificate-folder', type=pathlib.Path, help='Folder containing TLS artifacts.')
    parser.add_argument('--ssl-password', help='Password for encrypted TLS private key.')
    parser.add_argument('--network-delay', type=float, help='Network delay to use in seconds.', default=0.1)
    parser.add_argument(
        '--timeout-ref-provider',
        type=float,
        help='Time in seconds to wait for reference provider to be started.',
        default=10.0,
    )
    parser.add_argument('--no-1a', action='store_true', help='Do not execute test step 1a.')

    args = parser.parse_args()
    passed = run_ref_test(
        ip=args.ip,
        epr=args.epr,
        certificate_folder=args.certificate_folder,
        certificate_password=args.ssl_password,
        network_delay=args.network_delay,
        timeout_ref_provider=args.timeout_ref_provider,
        execute_1a=not args.no_1a,
    )
    sys.exit(0 if passed else 1)
