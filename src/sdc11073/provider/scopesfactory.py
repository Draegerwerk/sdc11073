"""The module implements the function mk_scopes."""

import urllib.parse
from urllib.parse import quote_plus

from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
from sdc11073.xml_types.wsd_types import ScopesType

KEY_PURPOSE_SERVICE_PROVIDER = 'sdc.mds.pkp:1.2.840.10004.20701.1.1'


def mk_scopes(mdib: ProviderMdibProtocol) -> ScopesType:
    """Return a ScopesType instance.

    This method creates the scopes for publishing in wsdiscovery.
    """
    pm_types = mdib.data_model.pm_types
    pm_names = mdib.data_model.pm_names
    scope = ScopesType()
    for nodetype, scheme in (
        (pm_names.LocationContextDescriptor, 'sdc.ctxt.loc'),
        (pm_names.OperatorContextDescriptor, 'sdc.ctxt.opr'),
        (pm_names.EnsembleContextDescriptor, 'sdc.ctxt.ens'),
        (pm_names.WorkflowContextDescriptor, 'sdc.ctxt.wfl'),
        (pm_names.MeansContextDescriptor, 'sdc.ctxt.mns'),
    ):
        entities = mdib.entities.by_node_type(nodetype)
        for entity in entities:
            for state in [
                s for s in entity.states.values() if s.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED
            ]:
                if not state.Identification:
                    msg = f'State {state.Handle} of type {nodetype} has no Identification element'
                    raise ValueError(msg)
                for ident in state.Identification:
                    # IEEE Std 11073-20701-2018 9.4 context based discovery
                    instance_identifier = f'/{quote_plus(ident.Root)}/{quote_plus(ident.Extension)}'
                    context_uri = f'{scheme}:{instance_identifier}'
                    query = ''
                    if nodetype == pm_names.LocationContextDescriptor:
                        if not state.LocationDetail:
                            msg = f'State {state.Handle} of type {nodetype} has no LocationDetail element'
                            raise ValueError(msg)
                        query = urllib.parse.urlencode(
                            {
                                'fac': state.LocationDetail.Facility,
                                'bldng': state.LocationDetail.Building,
                                'flr': state.LocationDetail.Floor,
                                'poc': state.LocationDetail.PoC,
                                'rm': state.LocationDetail.Room,
                                'bed': state.LocationDetail.Bed,
                            }
                        )
                    if query:
                        context_uri = f'{context_uri}?{query}'
                    scope.text.append(context_uri)

    scope.text.extend(_get_device_component_based_scopes(mdib))
    scope.text.append(KEY_PURPOSE_SERVICE_PROVIDER)  # default scope that is always included
    return scope


def _get_device_component_based_scopes(mdib: ProviderMdibProtocol) -> set[str]:
    """Return a set of scope strings.

    SDC: For every instance derived from pm:AbstractComplexDeviceComponentDescriptor in the MDIB an
    SDC SERVICE PROVIDER SHOULD include a URI-encoded pm:AbstractComplexDeviceComponentDescriptor/pm:Type
    as dpws:Scope of the MDPWS discovery messages. The URI encoding conforms to the given Extended Backus-Naur Form.
    E.G.  sdc.cdc.type:///69650, sdc.cdc.type:/urn:oid:1.3.6.1.4.1.3592.2.1.1.0//DN_VMD

    Use only MDSDescriptor, because there might be alot of VmdDescriptor that might exceed the dpws message size limit.
    Also, VmdDescriptor do not contain relevant information for discovery purposes.

    :return: a set of scope strings
    """
    pm_types = mdib.data_model.pm_types
    pm_names = mdib.data_model.pm_names
    scopes = set()
    entities = mdib.entities.by_node_type(pm_names.MdsDescriptor)
    for entity in entities:
        if entity.descriptor.Type is not None:
            coding_systems = (
                ''
                if entity.descriptor.Type.CodingSystem == pm_types.DEFAULT_CODING_SYSTEM
                else entity.descriptor.Type.CodingSystem
            )
            csv = entity.descriptor.Type.CodingSystemVersion or ''
            scope_string = f'sdc.cdc.type:/{coding_systems}/{csv}/{entity.descriptor.Type.Code}'
            scopes.add(scope_string)
    return scopes
