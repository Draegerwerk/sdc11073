"""Run the reference tests."""

from __future__ import annotations

import argparse
import json
import logging
import logging.config
import pathlib
import sys
import threading
import time
import uuid
from typing import TYPE_CHECKING

from pat.ReferenceTestV2 import common, reference_consumer_v2, reference_provider_v2
from pat.ReferenceTestV2.consumer import result_collector

if TYPE_CHECKING:
    import os

    import sdc11073.certloader


def run(
    mdib_path: os.PathLike[str],
    adapter: str,
    epr: str,
    ssl_context_container: sdc11073.certloader.SSLContextContainer | None,
) -> None:
    """Run tests."""
    with pathlib.Path(__file__).parent.joinpath('logging_default.json').open() as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    threading.Thread(
        target=reference_provider_v2.run_provider,
        args=(mdib_path, adapter, epr, ssl_context_container),
        daemon=True,
    ).start()
    time.sleep(10)
    reference_consumer_v2.run_ref_test(adapter=adapter, epr=epr, ssl_context_container=ssl_context_container)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run plug-a-thon tests')
    parser.add_argument('--tls', action='store_true', help='Indicates whether tls encryption should be enabled.')
    parser.add_argument('--adapter', help='Network adapter IP address to use.', default='127.0.0.1')
    parser.add_argument(
        '--epr',
        help='Explicit endpoint reference to search for.',
        default=f'urn:uuid:{uuid.uuid4()}',
    )
    parser.add_argument(
        '--certificate-folder',
        type=pathlib.Path,
        help='Folder containing TLS artifacts.',
        default=pathlib.Path(__file__).parent.parent.joinpath('certs').resolve(),
    )
    parser.add_argument('--ssl-password', help='Password for encrypted TLS private key.', default='dummypass')

    parser.add_argument(
        '--mdib-path',
        type=pathlib.Path,
        help='Override MDIB file used by the provider.',
        default=pathlib.Path(__file__).parent.joinpath('PlugathonMdibV2.xml'),
    )

    args = parser.parse_args()
    run(
        mdib_path=args.mdib_path,
        adapter=args.adapter,
        epr=args.epr,
        ssl_context_container=common.get_ssl_context(args.certificate_folder, args.ssl_password) if args.tls else None,
    )
    result_collector.ResultCollector.print_summary()
    sys.exit(bool(result_collector.ResultCollector.failed))
