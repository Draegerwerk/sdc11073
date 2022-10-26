from dataclasses import dataclass
from typing import List, Tuple
from typing import Type
from urllib.parse import quote_plus

from .hostedserviceimpl import DPWSHostedService
from .services.servicesbase import DPWSPortTypeImpl
from .. import wsdiscovery
from ..location import SdcLocation

def mk_scopes(mdib) -> List[wsdiscovery.Scope]:
    """ scopes factory
    This method creates the scopes for publishing in wsdiscovery.
    :param mdib:
    :return: list of scopes
    """
    scopes = []
    pm_types = mdib.data_model.pmtypes
    pm_names = mdib.data_model.pm_names
    locations = mdib.context_states.NODETYPE.get(pm_names.LocationContextState, [])
    assoc_loc = [l for l in locations if l.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
    for loc in assoc_loc:
        det = loc.LocationDetail
        dr_loc = SdcLocation(fac=det.Facility, poc=det.PoC, bed=det.Bed, bld=det.Building,
                             flr=det.Floor, rm=det.Room)
        scopes.append(wsdiscovery.Scope(dr_loc.scope_string))

    for nodetype, scheme in (
            (pm_names.OperatorContextDescriptor, 'sdc.ctxt.opr'),
            (pm_names.EnsembleContextDescriptor, 'sdc.ctxt.ens'),
            (pm_names.WorkflowContextDescriptor, 'sdc.ctxt.wfl'),
            (pm_names.MeansContextDescriptor, 'sdc.ctxt.mns'),
    ):
        descriptors = mdib.descriptions.NODETYPE.get(nodetype, [])
        for descriptor in descriptors:
            states = mdib.context_states.descriptorHandle.get(descriptor.Handle, [])
            assoc_st = [s for s in states if s.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
            for state in assoc_st:
                for idnt in state.Identification:
                    scopes.append(wsdiscovery.Scope(f'{scheme}:/{quote_plus(idnt.Root)}/{quote_plus(idnt.Extension)}'))

    scopes.extend(_get_device_component_based_scopes(mdib))
    scopes.append(wsdiscovery.Scope('sdc.mds.pkp:1.2.840.10004.20701.1.1'))  # key purpose Service provider
    return scopes


def _get_device_component_based_scopes(mdib):
    """
    SDC: For every instance derived from pm:AbstractComplexDeviceComponentDescriptor in the MDIB an
    SDC SERVICE PROVIDER SHOULD include a URIencoded pm:AbstractComplexDeviceComponentDescriptor/pm:Type
    as dpws:Scope of the MDPWS discovery messages. The URI encoding conforms to the given Extended Backus-Naur Form.
    E.G.  sdc.cdc.type:///69650, sdc.cdc.type:/urn:oid:1.3.6.1.4.1.3592.2.1.1.0//DN_VMD
    After discussion with David: use only MDSDescriptor, VmdDescriptor makes no sense.
    :return: a set of scopes
    """
    pm_types = mdib.data_model.pmtypes
    pm_names = mdib.data_model.pm_names
    scopes = set()
    descriptors = mdib.descriptions.NODETYPE.get(pm_names.MdsDescriptor)
    for descriptor in descriptors:
        if descriptor.Type is not None:
            coding_systems = '' if descriptor.Type.CodingSystem == pm_types.DEFAULT_CODING_SYSTEM \
                else descriptor.Type.CodingSystem
            csv = descriptor.Type.CodingSystemVersion or ''
            scope = wsdiscovery.Scope(f'sdc.cdc.type:/{coding_systems}/{csv}/{descriptor.Type.Code}')
            scopes.add(scope)
    return scopes


@dataclass(frozen=True)
class HostedServices:
    dpws_hosted_services: Tuple[DPWSHostedService, ...]
    get_service: Type[DPWSPortTypeImpl]
    set_service: Type[DPWSPortTypeImpl] = None
    context_service: Type[DPWSPortTypeImpl] = None
    description_event_service: Type[DPWSPortTypeImpl] = None
    state_event_service: Type[DPWSPortTypeImpl] = None
    waveform_service: Type[DPWSPortTypeImpl] = None
    containment_tree_service: Type[DPWSPortTypeImpl] = None
    localization_service: Type[DPWSPortTypeImpl] = None


def mk_all_services(sdc_device, components, sdc_definitions) -> HostedServices:
    # register all services with their endpoint references acc. to sdc standard
    actions = sdc_definitions.Actions
    service_handlers_lookup = components.service_handlers
    cls = service_handlers_lookup['GetService']
    get_service = cls('GetService', sdc_device)
    cls = service_handlers_lookup['LocalizationService']
    localization_service = cls('LocalizationService', sdc_device)
    offered_subscriptions = []
    get_service_hosted = DPWSHostedService(sdc_device, 'Get',
                                           components.msg_dispatch_method,
                                           [get_service, localization_service],
                                           offered_subscriptions)

    # grouped acc to sdc REQ 0035
    cls = service_handlers_lookup['ContextService']
    context_service = cls('ContextService', sdc_device)
    cls = service_handlers_lookup['DescriptionEventService']
    description_event_service = cls('DescriptionEventService', sdc_device)
    cls = service_handlers_lookup['StateEventService']
    state_event_service = cls('StateEventService', sdc_device)
    cls = service_handlers_lookup['WaveformService']
    waveform_service = cls('WaveformService', sdc_device)

    offered_subscriptions = [actions.EpisodicContextReport,
                             actions.DescriptionModificationReport,
                             actions.EpisodicMetricReport,
                             actions.EpisodicAlertReport,
                             actions.EpisodicComponentReport,
                             actions.EpisodicOperationalStateReport,
                             actions.Waveform,
                             actions.SystemErrorReport,
                             actions.PeriodicMetricReport,
                             actions.PeriodicAlertReport,
                             actions.PeriodicContextReport,
                             actions.PeriodicComponentReport,
                             actions.PeriodicOperationalStateReport
                             ]

    sdc_service_hosted = DPWSHostedService(sdc_device, 'StateEvent',
                                           components.msg_dispatch_method,
                                           [context_service,
                                            description_event_service,
                                            state_event_service,
                                            waveform_service],
                                           offered_subscriptions)

    cls = service_handlers_lookup['SetService']
    set_dispatcher = cls('SetService', sdc_device)
    offered_subscriptions = [actions.OperationInvokedReport]

    set_service_hosted = DPWSHostedService(sdc_device, 'Set',
                                           components.msg_dispatch_method,
                                           [set_dispatcher],
                                           offered_subscriptions)

    cls = service_handlers_lookup['ContainmentTreeService']
    containment_tree_dispatcher = cls('ContainmentTreeService', sdc_device)
    offered_subscriptions = []
    containment_tree_service_hosted = DPWSHostedService(sdc_device, 'ContainmentTree',
                                                        components.msg_dispatch_method,
                                                        [containment_tree_dispatcher],
                                                        offered_subscriptions)
    dpws_services = (get_service_hosted,
                     sdc_service_hosted,
                     set_service_hosted,
                     containment_tree_service_hosted)
    hosted_services = HostedServices(dpws_services,
                                     get_service,
                                     set_service=set_dispatcher,
                                     context_service=context_service,
                                     description_event_service=description_event_service,
                                     state_event_service=state_event_service,
                                     waveform_service=waveform_service,
                                     containment_tree_service=containment_tree_dispatcher,
                                     localization_service=localization_service
                                     )
    return hosted_services


def mk_minimal_services_plus_loc(sdc_device, components,
                                 sdc_definitions) -> HostedServices:  # pylint: disable=unused-argument
    """"This example function instantiates only GetService and LocalizationService"""
    service_handlers_lookup = components.service_handlers

    cls = service_handlers_lookup['GetService']
    get_service = cls('GetService', sdc_device)
    cls = service_handlers_lookup['LocalizationService']
    localization_service = cls('LocalizationService', sdc_device)
    offered_subscriptions = []
    get_service_hosted = DPWSHostedService(sdc_device, 'Get',
                                           components.msg_dispatch_method,
                                           [get_service, localization_service],
                                           offered_subscriptions)

    dpws_services = (get_service_hosted,)
    hosted_services = HostedServices(dpws_services,
                                     get_service,
                                     localization_service=localization_service
                                     )
    return hosted_services
