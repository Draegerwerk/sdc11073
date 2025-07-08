"""The module implements the function mk_scopes."""

import urllib.parse

from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
from sdc11073.xml_types.wsd_types import ScopesType

# from IEEE Std 11073-20701-2018 chapter 9.3 SDC PARTICIPANT KEY PURPOSE based discovery
KEY_PURPOSE_SERVICE_PROVIDER = 'sdc.mds.pkp:1.2.840.10004.20701.1.1'
# from IEEE Std 11073-20701-2018 chapter 9.4 Context-based discovery
BICEPS_URI_UNK = 'biceps.uri.unk'


def _query_from_location_state(state) -> str:  # noqa: ANN001  typing is unknown here as it depends on the data_model in the mdib
    """Return a query string from a LocationContextStateContainer from GLUE 9.4.1.2."""
    query_dict: dict[str, str] = {}
    if state.LocationDetail is None:
        return ''
    if state.LocationDetail.Facility is not None:
        query_dict['fac'] = state.LocationDetail.Facility
    if state.LocationDetail.Building is not None:
        query_dict['bldng'] = state.LocationDetail.Building
    if state.LocationDetail.Floor is not None:
        query_dict['flr'] = state.LocationDetail.Floor
    if state.LocationDetail.PoC is not None:
        query_dict['poc'] = state.LocationDetail.PoC
    if state.LocationDetail.Room is not None:
        query_dict['rm'] = state.LocationDetail.Room
    if state.LocationDetail.Bed is not None:
        query_dict['bed'] = state.LocationDetail.Bed
    return urllib.parse.urlencode(query_dict, quote_via=urllib.parse.quote, safe='')


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
                    instance_identifier = (
                        f'/{urllib.parse.quote(ident.Root if ident.Root is not None else BICEPS_URI_UNK, safe="")}'
                    )
                    if ident.Extension:
                        instance_identifier += f'/{urllib.parse.quote(ident.Extension, safe="")}'
                    context_uri = f'{scheme}:{instance_identifier}'
                    query = ''
                    if nodetype == pm_names.LocationContextDescriptor:
                        if not state.LocationDetail:
                            msg = f'State {state.Handle} of type {nodetype} has no LocationDetail element'
                            raise ValueError(msg)
                        query = _query_from_location_state(state)
                    if query:
                        context_uri = f'{context_uri}?{query}'
                    scope.text.append(context_uri)

    scope.text.extend(_get_device_component_based_scopes(mdib))
    scope.text.append(KEY_PURPOSE_SERVICE_PROVIDER)  # default scope that is always included
    return scope


def _get_device_component_based_scopes(mdib: ProviderMdibProtocol) -> set[str]:
    """Return a set of scope strings.

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
