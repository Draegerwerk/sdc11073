"""Factory functions that create the hosted services of a provider."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sdc11073.provider.dpwshostedservice import DPWSHostedService

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sdc11073.provider.porttypes.porttypebase import DPWSPortTypeBase
    from sdc11073.provider.providerimpl import SdcProvider, SdcProviderComponents
    from sdc11073.provider.subscriptionmgr_base import SubscriptionManagerProtocol


@dataclass(frozen=True)
class HostedServices:
    """Container for all instantiated hosted services and port types.

    The references to the services are for convenience.
    """

    dpws_hosted_services: dict[str, DPWSHostedService]
    get_service: type[DPWSPortTypeBase]
    set_service: type[DPWSPortTypeBase] = None
    context_service: type[DPWSPortTypeBase] = None
    description_event_service: type[DPWSPortTypeBase] = None
    state_event_service: type[DPWSPortTypeBase] = None
    waveform_service: type[DPWSPortTypeBase] = None
    containment_tree_service: type[DPWSPortTypeBase] = None
    localization_service: type[DPWSPortTypeBase] = None


def mk_dpws_hosts(
    sdc_device: SdcProvider,
    components: SdcProviderComponents,
    dpws_hosted_service_cls: type[DPWSHostedService],
    subscription_managers: Mapping[str, SubscriptionManagerProtocol],
) -> tuple[dict, dict]:
    """Instantiate the DPWS hosted services defined by the components.

    :param sdc_device: the provider the services belong to
    :param components: the components definition listing the hosted services
    :param dpws_hosted_service_cls: the class used to instantiate each hosted service
    :param subscription_managers: subscription managers by host name
    :return: a tuple of (hosted services by host name, port type services by name)
    """
    dpws_services = {}
    services_by_name = {}
    for host_name, service_cls_list in components.hosted_services.items():
        services = []
        for service_cls in service_cls_list:
            service = service_cls(sdc_device)
            services.append(service)
            services_by_name[service.port_type_name.localname] = service
        subscription_manager = subscription_managers.get(host_name)
        hosted = dpws_hosted_service_cls(sdc_device, subscription_manager, host_name, services)
        dpws_services[host_name] = hosted
    return dpws_services, services_by_name


def mk_all_services(
    sdc_device: SdcProvider,
    components: SdcProviderComponents,
    subscription_managers: Mapping[str, SubscriptionManagerProtocol],
) -> HostedServices:
    """Instantiate all hosted services of the provider.

    :param sdc_device: the provider the services belong to
    :param components: the components definition listing the hosted services
    :param subscription_managers: subscription managers by host name
    :return: a HostedServices instance referencing all created services
    """
    # register all services with their endpoint references acc. to structure in components
    dpws_hosts, services_by_name = mk_dpws_hosts(sdc_device, components, DPWSHostedService, subscription_managers)
    return HostedServices(
        dpws_hosts,
        services_by_name['GetService'],
        set_service=services_by_name.get('SetService'),
        context_service=services_by_name.get('ContextService'),
        description_event_service=services_by_name.get('DescriptionEventService'),
        state_event_service=services_by_name.get('StateEventService'),
        waveform_service=services_by_name.get('WaveformService'),
        containment_tree_service=services_by_name.get('ContainmentTreeService'),
        localization_service=services_by_name.get('LocalizationService'),
    )
