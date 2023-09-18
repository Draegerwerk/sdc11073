from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Type, TYPE_CHECKING

from .dpwshostedservice import DPWSHostedService

if TYPE_CHECKING:
    from .porttypes.porttypebase import DPWSPortTypeBase


@dataclass(frozen=True)
class HostedServices:
    """This is a container for all instantiated hosted services and port types.
    The references to the services are for convenience."""
    dpws_hosted_services: Dict[str, DPWSHostedService]
    get_service: Type[DPWSPortTypeBase]
    set_service: Type[DPWSPortTypeBase] = None
    context_service: Type[DPWSPortTypeBase] = None
    description_event_service: Type[DPWSPortTypeBase] = None
    state_event_service: Type[DPWSPortTypeBase] = None
    waveform_service: Type[DPWSPortTypeBase] = None
    containment_tree_service: Type[DPWSPortTypeBase] = None
    localization_service: Type[DPWSPortTypeBase] = None


def mk_dpws_hosts(sdc_device, components, dpws_hosted_service_cls, subscription_managers: dict) -> (dict, dict):
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


def mk_all_services(sdc_device, components, subscription_managers) -> HostedServices:
    # register all services with their endpoint references acc. to structure in components
    dpws_hosts, services_by_name = mk_dpws_hosts(sdc_device, components, DPWSHostedService, subscription_managers)
    hosted_services = HostedServices(dpws_hosts,
                                     services_by_name['GetService'],
                                     set_service=services_by_name.get('SetService'),
                                     context_service=services_by_name.get('ContextService'),
                                     description_event_service=services_by_name.get('DescriptionEventService'),
                                     state_event_service=services_by_name.get('StateEventService'),
                                     waveform_service=services_by_name.get('WaveformService'),
                                     containment_tree_service=services_by_name.get('ContainmentTreeService'),
                                     localization_service=services_by_name.get('LocalizationService')
                                     )
    return hosted_services
