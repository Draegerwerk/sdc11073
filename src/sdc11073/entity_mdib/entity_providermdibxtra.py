"""The module contains extensions to the functionality of the EntityProviderMdib."""
from __future__ import annotations

import time
from collections import defaultdict
from typing import TYPE_CHECKING

from sdc11073.etc import apply_map
from sdc11073.exceptions import ApiUsageError
from sdc11073.xml_types.pm_types import RetrievabilityMethod

if TYPE_CHECKING:
    from sdc11073.location import SdcLocation
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractStateContainer
    from sdc11073.xml_types.pm_types import InstanceIdentifier

    from .entities import ProviderEntity, ProviderMultiStateEntity
    from .entity_providermdib import EntityProviderMdib


class EntityProviderMdibMethods:
    """Extra methods for provider mdib that are not part of core functionality."""

    def __init__(self, provider_mdib: EntityProviderMdib):
        self._mdib = provider_mdib
        self.default_validators = (provider_mdib.data_model.pm_types.InstanceIdentifier(
            root='rootWithNoMeaning', extension_string='System'),)

    def set_all_source_mds(self):
        """Set source mds in all entities."""
        dict_by_parent_handle = defaultdict(list)
        descriptor_containers = [entity.descriptor for entity in self._mdib.internal_entities.values()]
        for d in descriptor_containers:
            dict_by_parent_handle[d.parent_handle].append(d)

        def tag_tree(source_mds_handle: str, descriptor_container: AbstractDescriptorContainer):
            descriptor_container.set_source_mds(source_mds_handle)
            children = dict_by_parent_handle[descriptor_container.Handle]
            for ch in children:
                tag_tree(source_mds_handle, ch)

        for mds in dict_by_parent_handle[None]:  # only mds has no parent
            tag_tree(mds.Handle, mds)

    def set_location(self, sdc_location: SdcLocation,
                     validators: list[InstanceIdentifier] | None = None,
                     location_context_descriptor_handle: str | None = None):
        """Create a location context state. The new state will be the associated state.

        This method updates only the mdib data!
        Use the SdcProvider.set_location method if you want to publish the address on the network.
        :param sdc_location: a sdc11073.location.SdcLocation instance
        :param validators: a list of InstanceIdentifier objects or None
               If None, self.default_validators is used.
        :param location_context_descriptor_handle: Only needed if the mdib contains more than one
               LocationContextDescriptor. Then this defines the descriptor for which a new LocationContextState
               shall be created.
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names

        if location_context_descriptor_handle is None:
            # assume there is only one descriptor in mdib, user has not provided a handle.
            location_entities = mdib.entities.by_node_type(pm.LocationContextDescriptor)
            location_entity = None if len(location_entities) == 0 else location_entities[0]
        else:
            location_entity = mdib.entities.by_handle(location_context_descriptor_handle)

        if location_entity is None:
            raise ValueError('no LocationContextDescriptor entity found in mdib')

        new_location = location_entity.new_state()
        new_location.update_from_sdc_location(sdc_location)
        if validators is None:
            new_location.Validator = self.default_validators
        else:
            new_location.Validator = validators

        with mdib.context_state_transaction() as mgr:
            # disassociate before creating a new state
            handles = self.disassociate_all(location_entity,
                                            mgr.new_mdib_version,
                                            ignored_handle=new_location.Handle)
            new_location.BindingMdibVersion = mgr.new_mdib_version
            new_location.BindingStartTime = time.time()
            new_location.ContextAssociation = mdib.data_model.pm_types.ContextAssociation.ASSOCIATED
            handles.append(new_location.Handle)
            mgr.write_entity(location_entity, handles)

    def set_initial_content(self,
                            descriptor_containers: list[AbstractDescriptorContainer],
                            state_containers: list[AbstractStateContainer]):
        """Add states."""
        if self._mdib.is_initialized:  # pragma: no cover
            raise ApiUsageError('method "set_initial_content" can not be called when mdib is already initialized')
        for d in descriptor_containers:
            states = [s for s in state_containers if s.DescriptorHandle == d.Handle]
            entity = self._mdib.entity_factory(d, states)
            self._mdib.internal_entities[d.Handle] = entity

        self.set_all_source_mds()
        self.mk_state_containers_for_all_descriptors()
        self.set_states_initial_values()
        self.update_retrievability_lists()

    def mk_state_containers_for_all_descriptors(self):
        """Create a state container for every descriptor that is missing a state in mdib.

        The model requires that there is a state for every descriptor (exception: multi-states)
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        for entity in mdib.internal_entities.values():
            if entity.descriptor.is_context_descriptor:
                continue
            if entity.state is None:
                state_cls = mdib.data_model.get_state_class_for_descriptor(entity.descriptor)
                state = state_cls(entity.descriptor)
                entity.state = state
                # add some initial values where needed
                if state.is_alert_condition:
                    state.DeterminationTime = time.time()
                elif state.NODETYPE == pm.AlertSystemState:  # noqa: SIM300
                    state.LastSelfCheck = time.time()
                    state.SelfCheckCount = 1
                elif state.NODETYPE == pm.ClockState:  # noqa: SIM300
                    state.LastSet = time.time()
                if mdib.current_transaction is not None:
                    mdib.current_transaction.add_state(state)

    def set_states_initial_values(self):
        """Set all states to defined starting conditions.

        This method is ment to be called directly after the mdib was loaded and before the provider is published
        on the network.
        It changes values only internally in the mdib, no notifications are sent!

        """
        pm_names = self._mdib.data_model.pm_names
        pm_types = self._mdib.data_model.pm_types

        for entity in self._mdib.internal_entities.values():
            if entity.node_type == pm_names.AlertSystemDescriptor:
                # alert systems are active
                entity.state.ActivationState = pm_types.AlertActivation.ON
                entity.state.SystemSignalActivation.append(
                    pm_types.SystemSignalActivation(manifestation=pm_types.AlertSignalManifestation.AUD,
                                                    state=pm_types.AlertActivation.ON))
            elif entity.descriptor.is_alert_condition_descriptor:
                # alert conditions are active, but not present
                entity.state.ActivationState = pm_types.AlertActivation.ON
                entity.state.Presence = False
            elif entity.descriptor.is_alert_signal_descriptor:
                # alert signals are not present, and delegable signals are also not active
                if entity.descriptor.SignalDelegationSupported:
                    entity.state.Location = pm_types.AlertSignalPrimaryLocation.REMOTE
                    entity.state.ActivationState = pm_types.AlertActivation.OFF
                    entity.state.Presence = pm_types.AlertSignalPresence.OFF
                else:
                    entity.state.ActivationState = pm_types.AlertActivation.ON
                    entity.state.Presence = pm_types.AlertSignalPresence.OFF
            elif entity.descriptor.is_component_descriptor:
                # all components are active
                entity.state.ActivationState = pm_types.ComponentActivation.ON
            elif entity.descriptor.is_operational_descriptor:
                # all operations are enabled
                entity.state.OperatingMode = pm_types.OperatingMode.ENABLED

    def update_retrievability_lists(self):
        """Update internal lists, based on current mdib descriptors."""
        mdib = self._mdib
        with mdib.mdib_lock:
            del mdib._retrievability_episodic[:]  # noqa: SLF001
            mdib.retrievability_periodic.clear()
            for entity in mdib.internal_entities.values():
                for r in entity.descriptor.get_retrievability():
                    for r_by in r.By:
                        if r_by.Method == RetrievabilityMethod.EPISODIC:
                            mdib._retrievability_episodic.append(entity.descriptor.Handle)  # noqa: SLF001
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            mdib.retrievability_periodic[period_ms].append(entity.descriptor.Handle)

    def get_all_entities_in_subtree(self, root_entity: ProviderEntity | ProviderMultiStateEntity,
                                    depth_first: bool = True,
                                    include_root: bool = True,
                                    ) -> list[ProviderEntity | ProviderMultiStateEntity]:
        """Return the tree below descriptor_container as a flat list."""
        result = []

        def _getchildren(parent: ProviderEntity | ProviderMultiStateEntity):
            child_containers = [e for e in self._mdib.internal_entities.values() if e.parent_handle == parent.handle]
            if not depth_first:
                result.extend(child_containers)
            apply_map(_getchildren, child_containers)
            if depth_first:
                result.extend(child_containers)

        if include_root and not depth_first:
            result.append(root_entity)
        _getchildren(root_entity)
        if include_root and depth_first:
            result.append(root_entity)
        return result

    def disassociate_all(self,
                         entity: ProviderMultiStateEntity,
                         unbinding_mdib_version: int,
                         ignored_handle: str | None = None) -> list[str]:
        """Disassociate all associated states in entity.

        The method returns a list of states that were disassociated.
        :param entity: ProviderMultiStateEntity
        :param ignored_handle: the context state with this Handle shall not be touched.
        """
        pm_types = self._mdib.data_model.pm_types
        disassociated_state_handles = []
        for state in entity.states.values():
            if state.Handle == ignored_handle or state.ContextAssociation == pm_types.ContextAssociation.NO_ASSOCIATION:
                # If state is already part of this transaction leave it also untouched, accept what the user wanted.
                # If state is not associated, also do not touch it.
                continue
            if state.ContextAssociation != pm_types.ContextAssociation.DISASSOCIATED \
                    or state.UnbindingMdibVersion is None:
                state.ContextAssociation = pm_types.ContextAssociation.DISASSOCIATED
                if state.UnbindingMdibVersion is None:
                    state.UnbindingMdibVersion = unbinding_mdib_version
                    state.BindingEndTime = time.time()
                disassociated_state_handles.append(state.Handle)
        return disassociated_state_handles
