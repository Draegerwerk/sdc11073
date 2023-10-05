from urllib.parse import quote_plus

from sdc11073.location import SdcLocation
from sdc11073.xml_types.wsd_types import ScopesType


def mk_scopes(mdib) -> ScopesType:
    """ scopes factory
    This method creates the scopes for publishing in wsdiscovery.
    :param mdib:
    :return: wsdiscovery.Scope
    """
    pm_types = mdib.data_model.pm_types
    pm_names = mdib.data_model.pm_names
    scope = ScopesType()
    locations = mdib.context_states.NODETYPE.get(pm_names.LocationContextState, [])
    assoc_loc = [loc for loc in locations if loc.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
    if len(assoc_loc) == 1:
        loc = assoc_loc[0]
        det = loc.LocationDetail
        dr_loc = SdcLocation(fac=det.Facility, poc=det.PoC, bed=det.Bed, bldng=det.Building,
                             flr=det.Floor, rm=det.Room)
        scope.text.append(dr_loc.scope_string)

    for nodetype, scheme in ((pm_names.OperatorContextDescriptor, 'sdc.ctxt.opr'),
                             (pm_names.EnsembleContextDescriptor, 'sdc.ctxt.ens'),
                             (pm_names.WorkflowContextDescriptor, 'sdc.ctxt.wfl'),
                             (pm_names.MeansContextDescriptor, 'sdc.ctxt.mns')):
        descriptors = mdib.descriptions.NODETYPE.get(nodetype, [])
        for descriptor in descriptors:
            states = mdib.context_states.descriptor_handle.get(descriptor.Handle, [])
            assoc_st = [s for s in states if s.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
            for state in assoc_st:
                for ident in state.Identification:
                    scope.text.append(f'{scheme}:/{quote_plus(ident.Root)}/{quote_plus(ident.Extension)}')

    scope.text.extend(_get_device_component_based_scopes(mdib))
    scope.text.append('sdc.mds.pkp:1.2.840.10004.20701.1.1')  # key purpose Service provider
    return scope


def _get_device_component_based_scopes(mdib):
    """
    SDC: For every instance derived from pm:AbstractComplexDeviceComponentDescriptor in the MDIB an
    SDC SERVICE PROVIDER SHOULD include a URI-encoded pm:AbstractComplexDeviceComponentDescriptor/pm:Type
    as dpws:Scope of the MDPWS discovery messages. The URI encoding conforms to the given Extended Backus-Naur Form.
    E.G.  sdc.cdc.type:///69650, sdc.cdc.type:/urn:oid:1.3.6.1.4.1.3592.2.1.1.0//DN_VMD
    After discussion with David: use only MDSDescriptor, VmdDescriptor makes no sense.
    :return: a set of scopes
    """
    pm_types = mdib.data_model.pm_types
    pm_names = mdib.data_model.pm_names
    scopes = set()
    descriptors = mdib.descriptions.NODETYPE.get(pm_names.MdsDescriptor)
    for descriptor in descriptors:
        if descriptor.Type is not None:
            coding_systems = '' if descriptor.Type.CodingSystem == pm_types.DEFAULT_CODING_SYSTEM \
                else descriptor.Type.CodingSystem
            csv = descriptor.Type.CodingSystemVersion or ''
            scope_string = f'sdc.cdc.type:/{coding_systems}/{csv}/{descriptor.Type.Code}'
            scopes.add(scope_string)
    return scopes
