from __future__ import annotations

import datetime
import json
import logging.config
import os
import pathlib
import traceback
import uuid
from decimal import Decimal
from time import sleep
from typing import TYPE_CHECKING

import sdc11073
from sdc11073 import location, network
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.loghelper import LoggerAdapter
from sdc11073.mdib import ProviderMdib, descriptorcontainers
from sdc11073.provider import SdcProvider, components
from sdc11073.provider.servicesfactory import DPWSHostedService, HostedServices, mk_dpws_hosts
from sdc11073.provider.subscriptionmgr_async import SubscriptionsManagerReferenceParamAsync
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType

if TYPE_CHECKING:
    from sdc11073.provider.components import SdcProviderComponents

def get_network_adapter() -> network.NetworkAdapter:
    """Get network adapter from environment or first loopback."""
    if (ip := os.getenv('ref_ip')) is not None:  # noqa: SIM112
        return network.get_adapter_containing_ip(ip)
    # get next available loopback adapter
    return next(adapter for adapter in network.get_adapters() if adapter.is_loopback)


def get_location() -> location.SdcLocation:
    """Get location from environment or default."""
    return location.SdcLocation(
        fac=os.getenv('ref_fac', default='r_fac'),  # noqa: SIM112
        poc=os.getenv('ref_poc', default='r_poc'),  # noqa: SIM112
        bed=os.getenv('ref_bed', default='r_bed'),  # noqa: SIM112
    )


def get_ssl_context() -> sdc11073.certloader.SSLContextContainer | None:
    """Get ssl context from environment or None."""
    if (ca_folder := os.getenv('ref_ca')) is None:  # noqa: SIM112
        return None
    return mk_ssl_contexts_from_folder(
        ca_folder,
        private_key='user_private_key_encrypted.pem',
        certificate='user_certificate_root_signed.pem',
        ca_public_key='root_certificate.pem',
        cyphers_file=None,
        ssl_passwd=os.getenv('ref_ssl_passwd'),  # noqa:SIM112
    )


def get_epr() -> str:
    """Get epr from environment or default."""
    if (epr := os.getenv('ref_search_epr')) is not None:  # noqa: SIM112
        return epr
    return uuid.UUID('12345678-6f55-11ea-9697-123456789abc').urn


def get_mdib_path() -> pathlib.Path:
    """Get mdib from environment or default mdib."""
    if mdib_path := os.getenv('ref_mdib'):  # noqa:SIM112
        return pathlib.Path(mdib_path)
    return pathlib.Path(__file__).parent.joinpath('PlugathonMdibV2.xml')