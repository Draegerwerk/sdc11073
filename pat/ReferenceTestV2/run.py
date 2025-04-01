"""Script that executes the plug-a-thon tests."""

import os
import pathlib
import platform
import sys
import threading
import uuid

from pat.ReferenceTestV2 import reference_consumer_v2, reference_provider_v2
from sdc11073 import network


def setup(tls: bool):
    """Setups the run."""
    os.environ['ref_search_epr'] = str(uuid.uuid4())  # noqa: SIM112
    if platform.system() == 'Darwin':
        os.environ['ref_ip'] = next(str(adapter.ip) for adapter in network.get_adapters() if not adapter.is_loopback)  # noqa: SIM112
    else:
        os.environ['ref_ip'] = next(str(adapter.ip) for adapter in network.get_adapters() if adapter.is_loopback)  # noqa: SIM112
    if tls:
        certs_path = pathlib.Path(__file__).parent.parent.joinpath('certs')
        assert certs_path.exists()
        os.environ['ref_ca'] = str(certs_path)  # noqa: SIM112
        os.environ['ref_ssl_passwd'] = 'dummypass'  # noqa: S105,SIM112


def run() -> reference_consumer_v2.ResultsCollector:
    """Run tests."""
    threading.Thread(target=reference_provider_v2.run_provider, daemon=True).start()
    return reference_consumer_v2.run_ref_test(reference_consumer_v2.ResultsCollector())


def main(tls: bool) -> reference_consumer_v2.ResultsCollector:
    """Setups and run tests."""
    setup(tls)
    return run()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='run plug-a-thon tests')
    parser.add_argument('--tls', action='store_true', help='Indicates whether tls encryption should be enabled.')

    args = parser.parse_args()
    run_results = main(tls=args.tls)
    run_results.print_summary()
    sys.exit(bool(run_results.failed_count))
