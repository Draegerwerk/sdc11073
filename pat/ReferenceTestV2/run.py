"""Run the reference tests."""

import json
import logging
import logging.config
import os
import pathlib
import platform
import socket
import struct
import sys
import threading
import time
import uuid

from pat.ReferenceTestV2 import reference_consumer_v2, reference_provider_v2
from pat.ReferenceTestV2.consumer import result_collector
from sdc11073 import network

MULTICAST_PROBE = ('239.255.255.250', 3702)


def find_adapter_supporting_multicast() -> str:
    """Return the first adapter that can send multicast traffic to the WS-Discovery group."""
    adapters = network.get_adapters()
    for adapter in adapters:
        address = str(adapter.ip)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(address))
                sock.setblocking(False)
                _addr = struct.pack('4s4s', socket.inet_aton(MULTICAST_PROBE[0]), socket.inet_aton(address))
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, _addr)
                system = platform.system()
                if system != 'Windows':
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                sock.bind((address, MULTICAST_PROBE[1]))
                test_bytes = b'\0'
                sock.sendto(test_bytes, MULTICAST_PROBE)
                received_bytes = sock.recv(len(test_bytes))
                if test_bytes != received_bytes:
                    print(
                        f'Adapter address {address} cannot be used for multicast: expected',
                        test_bytes,
                        'got',
                        received_bytes,
                    )
                    continue
        except OSError as e:
            print(f'Adapter address {address} cannot be used for multicast', e)
            continue
        else:
            print(f'Adapter address {address} successfully used for multicast')
        return address
    raise RuntimeError('No network adapter found that can send multicast packets')


def setup(tls: bool):
    """Setups the run."""
    os.environ['ref_search_epr'] = f'urn:uuid:{uuid.uuid4()}'  # noqa: SIM112
    if not os.environ.get('ref_ip'):  # noqa: SIM112
        os.environ['ref_ip'] = find_adapter_supporting_multicast()  # noqa: SIM112
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
