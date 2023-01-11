from typing import List
from urllib.parse import quote_plus

from .. import wsdiscovery
from ..location import SdcLocation


def mk_scopes(mdib) -> List[wsdiscovery.Scope]:
    """ scopes factory
    This method creates the scopes for publishing in wsdiscovery.
    :param mdib:
    :return: list of scopes
    """
    scopes = []
    pm_types = mdib.data_model.pm_types
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
                for ident in state.Identification:
                    scopes.append(
                        wsdiscovery.Scope(f'{scheme}:/{quote_plus(ident.Root)}/{quote_plus(ident.Extension)}'))

    scopes.extend(_get_device_component_based_scopes(mdib))
    scopes.append(wsdiscovery.Scope('sdc.mds.pkp:1.2.840.10004.20701.1.1'))  # key purpose Service provider
    return scopes


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
            scope = wsdiscovery.Scope(f'sdc.cdc.type:/{coding_systems}/{csv}/{descriptor.Type.Code}')
            scopes.add(scope)
    return scopes
