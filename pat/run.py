"""Run the reference tests."""

from __future__ import annotations

import argparse
import json
import logging
import logging.config
import pathlib
import sys
import threading
import uuid
from concurrent import futures
from typing import TYPE_CHECKING

from pat import common, consumer, provider

if TYPE_CHECKING:
    import sdc11073.certloader


def run(
    adapter: str,
    epr: str,
    ssl_context_container: sdc11073.certloader.SSLContextContainer | None,
    network_delay: float,
) -> bool:
    """Run tests."""
    with pathlib.Path(__file__).parent.joinpath('logging_default.json').open() as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    # Run consumer in a thread pool to capture the boolean result

    with futures.ThreadPoolExecutor(max_workers=1) as pool:
        consumer_future = pool.submit(
            consumer.run_ref_test,
            adapter=adapter,
            epr=epr,
            ssl_context_container=ssl_context_container,
            execute_1a=True,
            network_delay=network_delay,
        )
        threading.Thread(
            target=provider.run_provider,
            args=(adapter, epr, ssl_context_container),
            daemon=True,
        ).start()
        return bool(consumer_future.result(timeout=60 * 3))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run plug-a-thon tests')
    parser.add_argument('--tls', action='store_true', help='Indicates whether tls encryption should be enabled.')
    parser.add_argument('--adapter', help='Network adapter IP address to use.', default='127.0.0.1')
    parser.add_argument(
        '--epr',
        help='Explicit endpoint reference to search for.',
        default=uuid.uuid4().urn,
    )
    parser.add_argument(
        '--certificate-folder',
        type=pathlib.Path,
        help='Folder containing TLS artifacts.',
        default=pathlib.Path(__file__).parent.parent.joinpath('certs').resolve(),
    )
    parser.add_argument('--ssl-password', help='Password for encrypted TLS private key.', default='dummypass')
    parser.add_argument('--network-delay', type=float, help='Network delay to use in seconds.', default=0.1)

    args = parser.parse_args()

    passed = run(
        adapter=args.adapter,
        epr=args.epr,
        ssl_context_container=common.get_ssl_context(args.certificate_folder, args.ssl_password) if args.tls else None,
        network_delay=args.network_delay,
    )
    sys.exit(0 if passed else 1)
