"""Script that executes the plug-a-thon tests."""
import json
import logging
import logging.config
import os
import pathlib
import platform
import sys
import threading
import time

from pat.ReferenceTestV2 import common, reference_consumer_v2, reference_provider_v2
from pat.ReferenceTestV2.consumer import result_collector
from sdc11073 import network


def setup(tls: bool):
    """Setups the run."""
    os.environ['ref_search_epr'] = common.get_epr()  # noqa: SIM112
    if platform.system() == 'Darwin':
        os.environ['ref_ip'] = next(str(adapter.ip) for adapter in network.get_adapters() if not adapter.is_loopback)  # noqa: SIM112
    else:
        os.environ['ref_ip'] = next(str(adapter.ip) for adapter in network.get_adapters() if adapter.is_loopback)  # noqa: SIM112
    os.environ['ref_ip'] = '127.0.0.1'  # noqa: SIM112
    if tls:
        certs_path = pathlib.Path(__file__).parent.parent.joinpath('certs')
        assert certs_path.exists()
        os.environ['ref_ca'] = str(certs_path)  # noqa: SIM112
        os.environ['ref_ssl_passwd'] = 'dummypass'  # noqa: S105,SIM112


def run() -> None:
    """Run tests."""
    with pathlib.Path(__file__).parent.joinpath('logging_default.json').open() as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    threading.Thread(target=reference_provider_v2.run_provider, daemon=True).start()
    time.sleep(10)
    reference_consumer_v2.run_ref_test()


def main(tls: bool) -> None:
    """Setups and run tests."""
    setup(tls)
    run()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='run plug-a-thon tests')
    parser.add_argument('--tls', action='store_true', help='Indicates whether tls encryption should be enabled.')

    args = parser.parse_args()
    main(args.tls)
    result_collector.ResultCollector.print_summary()
    sys.exit(bool(result_collector.ResultCollector.failed))
