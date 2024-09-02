from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, Union

from lxml import etree as etree_

from sdc11073 import observableproperties as properties
from sdc11073.mdib.mdibbase import MdibVersionGroup

if TYPE_CHECKING:
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter


class EntityMdibBase:
    metric_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    waveform_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    alert_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    context_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    component_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    new_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    updated_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    deleted_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    operation_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    sequence_id = properties.ObservableProperty()
    instance_id = properties.ObservableProperty()

    def __init__(self, sdc_definitions: type[BaseDefinitions],
                 logger: LoggerAdapter):
        """Construct MdibBase.

        :param sdc_definitions: a class derived from BaseDefinitions
        """
        self.sdc_definitions = sdc_definitions
        self.data_model = sdc_definitions.data_model
        self._logger = logger
        self.mdib_version = 0
        self.sequence_id = ''  # needs to be set to a reasonable value by derived class
        self.instance_id = None  # None or an unsigned int
        self.log_prefix = ''
        self.mdib_lock = Lock()

        self._get_mdib_response_node: Union[etree_.Element, None] = None
        self._mdib_node: Union[etree_.Element, None] = None
        self._md_description_node: Union[etree_.Element, None] = None
        self._md_state_node: Union[etree_.Element, None] = None

    @property
    def mdib_version_group(self) -> MdibVersionGroup:
        """"Get current version data."""
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    def _update_mdib_version_group(self, mdib_version_group: MdibVersionGroup):
        """Set members and update entries in DOM tree."""
        mdib_node = self._get_mdib_response_node[0]
        if mdib_version_group.mdib_version != self.mdib_version:
            self.mdib_version = mdib_version_group.mdib_version
            self._get_mdib_response_node.set('MdibVersion', str(mdib_version_group.mdib_version))
            mdib_node.set('MdibVersion', str(mdib_version_group.mdib_version))
        if mdib_version_group.sequence_id != self.sequence_id:
            self.sequence_id = mdib_version_group.sequence_id
            self._get_mdib_response_node.set('SequenceId', str(mdib_version_group.sequence_id))
            mdib_node.set('SequenceId', str(mdib_version_group.sequence_id))
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id
            self._get_mdib_response_node.set('InstanceId', str(mdib_version_group.instance_id))
            mdib_node.set('InstanceId', str(mdib_version_group.instance_id))

    @property
    def logger(self) -> LoggerAdapter:
        """Return the logger."""
        return self._logger
