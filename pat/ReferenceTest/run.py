"""Script that executes the plug-a-thon tests."""

import os
import pathlib
import platform
import sys
import threading
import uuid

from pat.ReferenceTest import reference_consumer, reference_provider
from sdc11073 import network


def setup(tls: bool):  # noqa: D103
    os.environ['REF_SEARCH_EPR'] = str(uuid.uuid4())
    if platform.system() == 'Darwin':
        os.environ['ref_ip'] = next(str(adapter.ip) for adapter in network.get_adapters() if not adapter.is_loopback)  # noqa: SIM112
    else:
        os.environ['ref_ip'] = next(str(adapter.ip) for adapter in network.get_adapters() if adapter.is_loopback)  # noqa: SIM112
    if tls:
        certs_path = pathlib.Path(__file__).parent.parent.joinpath('certs')
        assert certs_path.exists()
        os.environ['ref_ca'] = str(certs_path)  # noqa: SIM112
        os.environ['ref_ssl_passwd'] = 'dummypass'  # noqa: S105, SIM112


def run() -> reference_consumer.TestCollector:  # noqa: D103
    threading.Thread(target=reference_provider.run_provider, daemon=True).start()
    return reference_consumer.main()


def main(tls: bool):  # noqa: ANN201, D103
    setup(tls)
    return run()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='run plug-a-thon tests')
    parser.add_argument('--tls', action='store_true', help='Indicates whether tls encryption should be enabled.')

    args = parser.parse_args()
    run_results = main(tls=args.tls)
    sys.exit(run_results.overall_test_result is not reference_consumer.TestResult.PASSED)
