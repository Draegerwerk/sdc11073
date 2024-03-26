from __future__ import annotations

import inspect
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from sdc11073 import observableproperties as properties
from sdc11073.xml_types import ext_qnames as ext
from sdc11073.xml_types import msg_qnames as msg
from sdc11073.xml_types import pm_qnames, pm_types
from sdc11073.xml_types import xml_structure as x_struct

from .containerbase import ContainerBase

if TYPE_CHECKING:
    from collections.abc import Iterable
    from decimal import Decimal

    from lxml import etree as etree_

    from sdc11073 import xml_utils
    from sdc11073.namespaces import NamespaceHelper
    from sdc11073.xml_types.isoduration import DurationType
    from sdc11073.xml_types.xml_structure import ExtensionLocalValue


@dataclass(frozen=True)
class ChildDescriptorMapping:
    """Map element names to node types.

    Needed when building a xml tree of descriptors.
    The name of a child element is often not identical to the type of the descriptor, e.g. a channel uses
    pm.Metric for all classes derived from AbstractMetricDescriptor.
    """

    child_qname: etree_.QName
    node_types: tuple[etree_.QName, ...] = None

    def __repr__(self) -> str:
        if self.node_types is None:
            return f'{self.__class__.__name__} name={self.child_qname.localname} (no types)'
        types = ', '.join([t.localname for t in self.node_types])
        return f'{self.__class__.__name__} name={self.child_qname.localname} types={types}'


def sorted_child_data(obj: Any, member_name: str):
    """:return: an iterator with whatever the members have, starting with base class members"""
    classes = inspect.getmro(obj.__class__)
    for cls in reversed(classes):
        try:
            names = cls.__dict__[member_name]  # only access class member of this class, not parent
            for name in names:
                yield name
        except KeyError:
            continue


class AbstractDescriptorProtocol(Protocol):
    """The common Interface of all descriptors."""

    NODETYPE: etree_.QName
    STATE_QNAME: etree_.QName
    is_descriptor_container: bool
    is_system_context_descriptor: bool
    is_realtime_sample_array_metric_descriptor: bool
    is_metric_descriptor: bool
    is_operational_descriptor: bool
    is_component_descriptor: bool
    is_alert_descriptor: bool
    is_alert_signal_descriptor: bool
    is_alert_condition_descriptor: bool
    is_context_descriptor: bool
    is_leaf: bool  # determines if children can be added
    Handle: str
    DescriptorVersion: int
    SafetyClassification: pm_types.SafetyClassification
    Type: pm_types.CodedValue | None
    source_mds: str
    parent_handle: str | None
    coding: pm_types.Coding | None

    def __init__(self, handle: str, parent_handle: str | None):
        ...

    def set_source_mds(self, handle: str):
        """Set source_mds member."""
        ...

    @classmethod
    def from_node(cls, node: xml_utils.LxmlElement, parent_handle: str | None = None) -> AbstractDescriptorProtocol:
        """Create class and init its properties from the node."""


class AbstractDescriptorContainer(ContainerBase):
    """AbstractDescriptorContainer represents the AbstractDescriptor of BICEPS."""

    # these class variables allow easy type-checking. Derived classes will set corresponding values to True
    # pylint: disable=invalid-name
    is_descriptor_container = True
    is_system_context_descriptor = False
    is_realtime_sample_array_metric_descriptor = False
    is_metric_descriptor = False
    is_operational_descriptor = False
    is_component_descriptor = False
    is_alert_descriptor = False
    is_alert_signal_descriptor = False
    is_alert_condition_descriptor = False
    is_context_descriptor = False

    is_leaf = True  # determines if children can be added
    node = properties.ObservableProperty()  # the etree node

    Handle: str = x_struct.HandleAttributeProperty('Handle', is_optional=False)
    Extension: ExtensionLocalValue = x_struct.ExtensionNodeProperty(ext.Extension)
    DescriptorVersion: int = x_struct.VersionCounterAttributeProperty('DescriptorVersion',
                                                                      implied_py_value=0)
    SafetyClassification: pm_types.SafetyClassification = x_struct.EnumAttributeProperty('SafetyClassification',
                                                                                         implied_py_value=pm_types.SafetyClassification.INF,
                                                                                         enum_cls=pm_types.SafetyClassification)
    Type: pm_types.CodedValue | None = x_struct.SubElementProperty(pm_qnames.Type,
                                                                   value_class=pm_types.CodedValue,
                                                                   is_optional=True)
    # pylint: enable=invalid-name
    _props = ('Handle', 'DescriptorVersion', 'SafetyClassification', 'Extension', 'Type')
    _child_elements_order = (ext.Extension, pm_qnames.Type)  # child elements in BICEPS order
    STATE_QNAME = None
    extension_class_lookup: ClassVar[dict[etree_.QName, type[pm_types.PropertyBasedPMType]]] = {
        msg.Retrievability: pm_types.Retrievability
    }

    def __init__(self, handle: str | None, parent_handle: str | None):
        """Parent Handle can only be None for a Mds Descriptor. every other descriptor has a parent."""
        super().__init__()
        self._parent_handle = parent_handle
        self.Handle = handle
        self._source_mds = None  # needed on device side if mdib contains > 1 mds

    @property
    def coding(self) -> pm_types.Coding | None:  # noqa: D102
        return self.Type.coding if self.Type is not None else None

    @property
    def code_id(self) -> str | None:  # noqa: D102
        return self.Type.Code if self.Type is not None else None  # pylint:disable=no-member

    @property
    def coding_system(self) -> str | None:  # noqa: D102
        return self.Type.CodingSystem if self.Type is not None else None  # pylint:disable=no-member

    @property
    def parent_handle(self) -> str:  # noqa: D102
        return self._parent_handle

    @parent_handle.setter
    def parent_handle(self, value: str):
        self._parent_handle = value

    def get_retrievability(self) -> list[pm_types.Retrievability]:
        """Return all retrievability data from Extension."""
        return [pm_types.Retrievability.from_node(x) for x in self.Extension if x.tag == msg.Retrievability]

    def set_retrievability(self, retrievabilities: Iterable[pm_types.Retrievability]) -> None:
        """Replace all retrievability elements in Extension with provided ones."""
        for tmp in [x for x in self.Extension if x.tag == msg.Retrievability]:
            self.Extension.remove(tmp)
        self.Extension.extend([r.as_etree_node(msg.Retrievability, {}) for r in retrievabilities])

    def increment_descriptor_version(self):
        """Increment DescriptorVersion."""
        self.DescriptorVersion += 1

    def update_from_other_container(self, other: AbstractDescriptorContainer,
                                    skipped_properties: list[str] | None = None):
        """Update own properties with values from other descriptor."""
        if other.Handle != self.Handle:
            raise ValueError(
                f'Update from a container with different handle is not possible! '
                f'Have "{self.Handle}", got "{other.Handle}"')
        self._update_from_other(other, skipped_properties)

    def get_actual_value(self, attr_name: str) -> Any:
        """Ignores default value and implied value, e.g. returns None if value is not present in xml."""
        return getattr(self.__class__, attr_name).get_actual_value(self)

    def diff(self, other: AbstractDescriptorContainer, ignore_property_names: list[str] | None = None) -> None | list[
        str]:
        """Compare with another descriptor.

        It compares all properties plus the parent handle member.
        :param other: the object (descriptor container) to compare with
        :param ignore_property_names: list of properties that shall be excluded from diff calculation
        :return: textual representation of differences or None if equal.
        """
        ret = super().diff(other, ignore_property_names) or []
        if ignore_property_names is None or 'parent_handle' not in ignore_property_names:
            my_value = self.parent_handle
            try:
                other_value = other.parent_handle
            except AttributeError:
                ret.append(f'parent_handle={my_value}, other does not have this attribute')
            else:
                if my_value != other_value:
                    ret.append(f'parent_handle={my_value}, other={other_value}')
        return None if len(ret) == 0 else ret

    def tag_name_for_child_descriptor(self, node_type: etree_.QName) -> (etree_.QName, bool):
        """Determine the tag name of a child descriptor.

        This isneeded when the xml tree of the descriptor is created.
        It uses the _child_elements_order members of the class itself and its base classes
        which map node type to tag name.
        :param node_type: the type QName (NODETYPE member)
        :return: tuple(QName, set_xsi_type_flag).
        """
        for child in sorted_child_data(self, '_child_descriptor_name_mappings'):
            if child.node_types is not None and node_type in child.node_types:
                set_xsi_type = len(child.node_types) > 1
                return child.child_qname, set_xsi_type
        raise ValueError(f'{node_type} not known in child declarations of {self.__class__.__name__}')

    def sort_child_nodes(self, node: xml_utils.LxmlElement) -> None:
        """Bring all child elements of node in correct order (BICEPS schema).

        raises a ValueError if a child node exist that is not listed in ordered_tags
        :param node: the element to be sorted.
        """
        q_names = list(sorted_child_data(self, '_child_elements_order'))
        not_in_order = [n for n in node if n.tag not in q_names]
        if len(not_in_order) > 0:
            raise ValueError(f'{self.__class__.__name__}: not in Order:{[n.tag for n in not_in_order]} '
                             f'node={node.tag}, order={[o.localname for o in q_names]}')
        all_child_nodes = node[:]
        for child_node in all_child_nodes:
            node.remove(child_node)
        for q_name in q_names:
            for child_node in all_child_nodes:
                if child_node.tag == q_name:
                    node.append(child_node)

    def set_source_mds(self, handle: str):
        """Set source_mds member."""
        self._source_mds = handle

    @property
    def source_mds(self) -> str:
        """Get source mds handle."""
        return self._source_mds

    def __str__(self) -> str:
        name = self.NODETYPE.localname or None
        return (f'Descriptor "{name}": handle={self.Handle} descriptor version={self.DescriptorVersion} '
                f'parent handle={self.parent_handle}')

    def __repr__(self) -> str:
        name = self.NODETYPE.localname or None
        return (f'Descriptor "{name}": handle={self.Handle} descriptor version={self.DescriptorVersion} '
                f'parent={self.parent_handle}')

    @classmethod
    def from_node(cls, node: xml_utils.LxmlElement, parent_handle: str | None = None) -> AbstractDescriptorContainer:
        """Create class and init its properties from the node."""
        obj = cls(handle=None,  # will be determined in constructor from node value
                  parent_handle=parent_handle)
        obj.update_from_node(node)
        return obj


class AbstractDeviceComponentDescriptorContainer(AbstractDescriptorContainer):
    """Represents AbstractDeviceComponentDescriptor in BICEPS."""

    is_component_descriptor = True
    is_leaf = False
    ProductionSpecification: list[pm_types.ProductionSpecification] = x_struct.SubElementListProperty(
        pm_qnames.ProductionSpecification,
        value_class=pm_types.ProductionSpecification)
    _props = ('ProductionSpecification',)
    _child_elements_order = (pm_qnames.ProductionSpecification,)


class AbstractComplexDeviceComponentDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    """Represents AbstractComplexDeviceComponentDescriptor in BICEPS."""

    _child_elements_order = (pm_qnames.AlertSystem, pm_qnames.Sco)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.AlertSystem, (pm_qnames.AlertSystemDescriptor,)),
        ChildDescriptorMapping(pm_qnames.Sco, (pm_qnames.ScoDescriptor,)))


class MdsDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    """Represents MdsDescriptor in BICEPS."""

    NODETYPE = pm_qnames.MdsDescriptor
    STATE_QNAME = pm_qnames.MdsState
    MetaData = x_struct.SubElementProperty(pm_qnames.MetaData, value_class=pm_types.MetaData, is_optional=True)
    ApprovedJurisdictions: pm_types.ApprovedJurisdictions | None = x_struct.SubElementProperty(
        pm_qnames.ApprovedJurisdictions,
        value_class=pm_types.ApprovedJurisdictions,
        is_optional=True)
    _props = ('MetaData', 'ApprovedJurisdictions')
    _child_elements_order = (pm_qnames.MetaData,
                             pm_qnames.SystemContext,
                             pm_qnames.Clock,
                             pm_qnames.Battery,
                             pm_qnames.ApprovedJurisdictions,
                             pm_qnames.Vmd)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.SystemContext, (pm_qnames.SystemContextDescriptor,)),
        ChildDescriptorMapping(pm_qnames.Clock, (pm_qnames.ClockDescriptor,)),
        ChildDescriptorMapping(pm_qnames.Battery, (pm_qnames.BatteryDescriptor,)),
        ChildDescriptorMapping(pm_qnames.Vmd, (pm_qnames.VmdDescriptor,)),
    )


class VmdDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    """Represents VmdDescriptor in BICEPS."""

    NODETYPE = pm_qnames.VmdDescriptor
    STATE_QNAME = pm_qnames.VmdState

    ApprovedJurisdictions: pm_types.ApprovedJurisdictions | None = x_struct.SubElementProperty(
        pm_qnames.ApprovedJurisdictions,
        value_class=pm_types.ApprovedJurisdictions,
        is_optional=True)
    _props = ('ApprovedJurisdictions',)
    _child_elements_order = (pm_qnames.ApprovedJurisdictions,
                             pm_qnames.Channel)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.Channel, (pm_qnames.ChannelDescriptor,)),
    )


class ChannelDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    """Represents ChannelDescriptor in BICEPS."""

    NODETYPE = pm_qnames.ChannelDescriptor
    STATE_QNAME = pm_qnames.ChannelState
    _child_elements_order = (pm_qnames.Metric,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.Metric, (pm_qnames.NumericMetricDescriptor,
                                                  pm_qnames.StringMetricDescriptor,
                                                  pm_qnames.EnumStringMetricDescriptor,
                                                  pm_qnames.RealTimeSampleArrayMetricDescriptor,
                                                  pm_qnames.DistributionSampleArrayMetricDescriptor,
                                                  ),
                               ),
    )


class ClockDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    """Represents ClockDescriptor in BICEPS."""

    NODETYPE = pm_qnames.ClockDescriptor
    STATE_QNAME = pm_qnames.ClockState
    TimeProtocol: list[pm_types.CodedValue] = x_struct.SubElementListProperty(pm_qnames.TimeProtocol,
                                                                              value_class=pm_types.CodedValue)
    Resolution: DurationType | None = x_struct.DurationAttributeProperty('Resolution')
    _props = ('TimeProtocol', 'Resolution')
    _child_elements_order = (pm_qnames.TimeProtocol,)


class BatteryDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    """Represents BatteryDescriptor in BICEPS."""

    NODETYPE = pm_qnames.BatteryDescriptor
    STATE_QNAME = pm_qnames.BatteryState
    # pylint: disable=invalid-name
    CapacityFullCharge: pm_types.Measurement | None = x_struct.SubElementProperty(pm_qnames.CapacityFullCharge,
                                                                                  value_class=pm_types.Measurement,
                                                                                  is_optional=True)
    CapacitySpecified: pm_types.Measurement | None = x_struct.SubElementProperty(pm_qnames.CapacitySpecified,
                                                                                 value_class=pm_types.Measurement,
                                                                                 is_optional=True)
    VoltageSpecified: pm_types.Measurement | None = x_struct.SubElementProperty(pm_qnames.VoltageSpecified,
                                                                                value_class=pm_types.Measurement,
                                                                                is_optional=True)
    # pylint: enable=invalid-name
    _props = ('CapacityFullCharge', 'CapacitySpecified', 'VoltageSpecified')
    _child_elements_order = (pm_qnames.CapacityFullCharge,
                             pm_qnames.CapacitySpecified,
                             pm_qnames.VoltageSpecified)


class ScoDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    """Represents ScoDescriptor in BICEPS."""

    NODETYPE = pm_qnames.ScoDescriptor
    STATE_QNAME = pm_qnames.ScoState
    _child_elements_order = (pm_qnames.Operation,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.Operation, (pm_qnames.SetValueOperationDescriptor,
                                                     pm_qnames.SetStringOperationDescriptor,
                                                     pm_qnames.SetContextStateOperationDescriptor,
                                                     pm_qnames.SetMetricStateOperationDescriptor,
                                                     pm_qnames.SetComponentStateOperationDescriptor,
                                                     pm_qnames.SetAlertStateOperationDescriptor,
                                                     pm_qnames.ActivateOperationDescriptor,
                                                     ),
                               ),
    )


class AbstractMetricDescriptorContainer(AbstractDescriptorContainer):
    """Represents AbstractMetricDescriptor in BICEPS."""

    is_metric_descriptor = True
    Unit: pm_types.CodedValue = x_struct.SubElementProperty(pm_qnames.Unit, value_class=pm_types.CodedValue)
    BodySite: list[pm_types.CodedValue] = x_struct.SubElementListProperty(pm_qnames.BodySite,
                                                                          value_class=pm_types.CodedValue)
    Relation: list[pm_types.Relation] = x_struct.SubElementListProperty(pm_qnames.Relation,
                                                                        value_class=pm_types.Relation)
    MetricCategory: pm_types.MetricCategory = x_struct.EnumAttributeProperty('MetricCategory',
                                                                             enum_cls=pm_types.MetricCategory,
                                                                             default_py_value=pm_types.MetricCategory.UNSPECIFIED,
                                                                             is_optional=False)
    DerivationMethod: pm_types.DerivationMethod | None = x_struct.EnumAttributeProperty('DerivationMethod',
                                                                                        enum_cls=pm_types.DerivationMethod)
    #  There is an implied value defined, but it is complicated, therefore here not implemented:
    # - If pm:AbstractDescriptor/@MetricCategory is "Set" or "Preset", then the default value of DerivationMethod is "Man"
    # - If pm:AbstractDescriptor/@MetricCategory is "Clc", "Msrmt", "Rcmm", then the default value of DerivationMethod is "Auto"
    # - If pm:AbstractDescriptor/@MetricCategory is "Unspec", then no default value is being implied</xsd:documentation>
    MetricAvailability: pm_types.MetricAvailability = x_struct.EnumAttributeProperty('MetricAvailability',
                                                                                     enum_cls=pm_types.MetricAvailability,
                                                                                     default_py_value=pm_types.MetricAvailability.CONTINUOUS,
                                                                                     is_optional=False)
    MaxMeasurementTime: DurationType | None = x_struct.DurationAttributeProperty('MaxMeasurementTime')
    MaxDelayTime: DurationType | None = x_struct.DurationAttributeProperty('MaxDelayTime')
    DeterminationPeriod: DurationType | None = x_struct.DurationAttributeProperty('DeterminationPeriod')
    LifeTimePeriod: DurationType | None = x_struct.DurationAttributeProperty('LifeTimePeriod')
    ActivationDuration: DurationType | None = x_struct.DurationAttributeProperty('ActivationDuration')
    _props = (
        'Unit', 'BodySite', 'Relation', 'MetricCategory', 'DerivationMethod', 'MetricAvailability',
        'MaxMeasurementTime',
        'MaxDelayTime', 'DeterminationPeriod', 'LifeTimePeriod', 'ActivationDuration')
    _child_elements_order = (pm_qnames.Unit,
                             pm_qnames.BodySite,
                             pm_qnames.Relation)


class NumericMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    """Represents NumericMetricDescriptor in BICEPS."""

    NODETYPE = pm_qnames.NumericMetricDescriptor
    STATE_QNAME = pm_qnames.NumericMetricState
    TechnicalRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm_qnames.TechnicalRange,
                                                                           value_class=pm_types.Range)
    Resolution: Decimal = x_struct.DecimalAttributeProperty('Resolution', is_optional=False)
    AveragingPeriod: DurationType | None = x_struct.DurationAttributeProperty('AveragingPeriod')
    _props = ('TechnicalRange', 'Resolution', 'AveragingPeriod')
    _child_elements_order = (pm_qnames.TechnicalRange,)


class StringMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    """Represents StringMetricDescriptor in BICEPS."""

    NODETYPE = pm_qnames.StringMetricDescriptor
    STATE_QNAME = pm_qnames.StringMetricState


class EnumStringMetricDescriptorContainer(StringMetricDescriptorContainer):
    """Represents EnumStringMetricDescriptor in BICEPS."""

    NODETYPE = pm_qnames.EnumStringMetricDescriptor
    STATE_QNAME = pm_qnames.EnumStringMetricState
    AllowedValue: list[pm_types.AllowedValue] = x_struct.SubElementListProperty(pm_qnames.AllowedValue,
                                                                                value_class=pm_types.AllowedValue)
    _props = ('AllowedValue',)
    _child_elements_order = (pm_qnames.AllowedValue,)


class RealTimeSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    """Represents RealTimeSampleArrayMetricDescriptor in BICEPS."""

    is_realtime_sample_array_metric_descriptor = True
    NODETYPE = pm_qnames.RealTimeSampleArrayMetricDescriptor
    STATE_QNAME = pm_qnames.RealTimeSampleArrayMetricState
    TechnicalRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm_qnames.TechnicalRange,
                                                                           value_class=pm_types.Range)
    Resolution: Decimal = x_struct.DecimalAttributeProperty('Resolution', is_optional=False)
    SamplePeriod: DurationType = x_struct.DurationAttributeProperty('SamplePeriod', is_optional=False)
    _props = ('TechnicalRange', 'Resolution', 'SamplePeriod')
    _child_elements_order = (pm_qnames.TechnicalRange,)


class DistributionSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    """Represents DistributionSampleArrayMetricDescriptor in BICEPS."""

    NODETYPE = pm_qnames.DistributionSampleArrayMetricDescriptor
    STATE_QNAME = pm_qnames.DistributionSampleArrayMetricState
    TechnicalRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm_qnames.TechnicalRange,
                                                                           value_class=pm_types.Range)
    DomainUnit: pm_types.CodedValue = x_struct.SubElementProperty(pm_qnames.DomainUnit, value_class=pm_types.CodedValue)
    DistributionRange: pm_types.Range = x_struct.SubElementProperty(pm_qnames.DistributionRange,
                                                                    value_class=pm_types.Range,
                                                                    default_py_value=pm_types.Range())
    Resolution: Decimal = x_struct.DecimalAttributeProperty('Resolution', is_optional=False)
    _props = ('TechnicalRange', 'DomainUnit', 'DistributionRange', 'Resolution')
    _child_elements_order = (pm_qnames.TechnicalRange,
                             pm_qnames.DomainUnit,
                             pm_qnames.DistributionRange)


class AbstractOperationDescriptorContainer(AbstractDescriptorContainer):
    """Represents AbstractOperationDescriptor in BICEPS."""

    is_operational_descriptor = True
    OperationTarget: str = x_struct.HandleRefAttributeProperty('OperationTarget', is_optional=False)
    MaxTimeToFinish: DurationType | None = x_struct.DurationAttributeProperty('MaxTimeToFinish')
    InvocationEffectiveTimeout: DurationType | None = x_struct.DurationAttributeProperty(
        'InvocationEffectiveTimeout')
    Retriggerable: bool = x_struct.BooleanAttributeProperty('Retriggerable', implied_py_value=True)
    AccessLevel: pm_types.AccessLevel = x_struct.EnumAttributeProperty('AccessLevel',
                                                                       implied_py_value=pm_types.AccessLevel.USER,
                                                                       enum_cls=pm_types.AccessLevel)
    _props = ('OperationTarget', 'MaxTimeToFinish', 'InvocationEffectiveTimeout', 'Retriggerable', 'AccessLevel')


class AbstractOperationDescriptorProtocol(AbstractDescriptorProtocol):
    """Protocol definition for AbstractOperationDescriptorContainer."""

    OperationTarget: str
    MaxTimeToFinish: DurationType | None
    InvocationEffectiveTimeout: DurationType | None
    Retriggerable: bool
    AccessLevel: pm_types.AccessLevel


class SetValueOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    """Represents SetValueOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.SetValueOperationDescriptor
    STATE_QNAME = pm_qnames.SetValueOperationState


class SetStringOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    """Represents SetStringOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.SetStringOperationDescriptor
    STATE_QNAME = pm_qnames.SetStringOperationState
    MaxLength: int | None = x_struct.IntegerAttributeProperty('MaxLength')
    _props = ('MaxLength',)


class AbstractSetStateOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    """Represents AbstractSetStateOperationDescriptor in BICEPS."""

    ModifiableData: list[str] = x_struct.SubElementStringListProperty(pm_qnames.ModifiableData)
    _props = ('ModifiableData',)
    _child_elements_order = (pm_qnames.ModifiableData,)


class SetContextStateOperationDescriptorContainer(AbstractSetStateOperationDescriptorContainer):
    """Represents SetContextStateOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.SetContextStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetContextStateOperationState


class SetMetricStateOperationDescriptorContainer(AbstractSetStateOperationDescriptorContainer):
    """Represents SetMetricStateOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.SetMetricStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetMetricStateOperationState


class SetComponentStateOperationDescriptorContainer(AbstractSetStateOperationDescriptorContainer):
    """Represents SetComponentStateOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.SetComponentStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetComponentStateOperationState


class SetAlertStateOperationDescriptorContainer(AbstractSetStateOperationDescriptorContainer):
    """Represents SetAlertStateOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.SetAlertStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetAlertStateOperationState


class ActivateOperationDescriptorContainer(AbstractSetStateOperationDescriptorContainer):
    """Represents ActivateOperationDescriptor in BICEPS."""

    NODETYPE = pm_qnames.ActivateOperationDescriptor
    STATE_QNAME = pm_qnames.ActivateOperationState
    Argument: list[pm_types.ActivateOperationDescriptorArgument] = x_struct.SubElementListProperty(
        pm_qnames.Argument, value_class=pm_types.ActivateOperationDescriptorArgument)
    _props = ('Argument',)
    _child_elements_order = (pm_qnames.Argument,)


class AbstractAlertDescriptorContainer(AbstractDescriptorContainer):
    """Represents AbstractAlertDescriptor in BICEPS."""

    is_alert_descriptor = True
    is_leaf = False


class AlertSystemDescriptorContainer(AbstractAlertDescriptorContainer):
    """Represents AlertSystemDescriptor in BICEPS.

    AlertSystemDescriptor describes an Alert system to detect Alert conditions and generate Alert signals,
    which belong to specific Alert conditions.
    Alert conditions are represented by a list of pm:AlertConditionDescriptor elements and
    Alert signals are represented by a list of pm:AlertSignalDescriptor elements.
    """

    NODETYPE = pm_qnames.AlertSystemDescriptor
    STATE_QNAME = pm_qnames.AlertSystemState
    MaxPhysiologicalParallelAlarms: int | None = x_struct.UnsignedIntAttributeProperty('MaxPhysiologicalParallelAlarms')
    MaxTechnicalParallelAlarms: int | None = x_struct.UnsignedIntAttributeProperty('MaxTechnicalParallelAlarms')
    SelfCheckPeriod: DurationType | None = x_struct.DurationAttributeProperty('SelfCheckPeriod')
    _props = ('MaxPhysiologicalParallelAlarms', 'MaxTechnicalParallelAlarms', 'SelfCheckPeriod')
    _child_elements_order = (pm_qnames.AlertCondition,
                             pm_qnames.AlertSignal)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.AlertCondition, (pm_qnames.AlertConditionDescriptor,
                                                          pm_qnames.LimitAlertConditionDescriptor,
                                                          ),
                               ),
        ChildDescriptorMapping(pm_qnames.AlertSignal, (pm_qnames.AlertSignalDescriptor,)),
    )


class AlertConditionDescriptorContainer(AbstractAlertDescriptorContainer):
    """Represents AlertConditionDescriptor in BICEPS.

    An ALERT CONDITION contains the information about a potentially or actually HAZARDOUS SITUATION.
    Examples: a physiological alarm limit has been exceeded or a sensor has been unplugged.
    """

    is_alert_condition_descriptor = True
    NODETYPE = pm_qnames.AlertConditionDescriptor
    STATE_QNAME = pm_qnames.AlertConditionState
    Source: list[str] = x_struct.SubElementHandleRefListProperty(
        pm_qnames.Source)  # a list of 0...n pm:HandleRef elements
    CauseInfo: list[pm_types.CauseInfo] = x_struct.SubElementListProperty(pm_qnames.CauseInfo,
                                                                          value_class=pm_types.CauseInfo)
    Kind: pm_types.AlertConditionKind = x_struct.EnumAttributeProperty('Kind',
                                                                       default_py_value=pm_types.AlertConditionKind.OTHER,
                                                                       enum_cls=pm_types.AlertConditionKind,
                                                                       is_optional=False)
    Priority: pm_types.AlertConditionPriority = x_struct.EnumAttributeProperty('Priority',
                                                                               default_py_value=pm_types.AlertConditionPriority.NONE,
                                                                               enum_cls=pm_types.AlertConditionPriority,
                                                                               is_optional=False)
    DefaultConditionGenerationDelay: DurationType = x_struct.DurationAttributeProperty(
        'DefaultConditionGenerationDelay',
        implied_py_value=0)
    CanEscalate: pm_types.CanEscalate | None = x_struct.EnumAttributeProperty(
        'CanEscalate', enum_cls=pm_types.CanEscalate)
    CanDeescalate: pm_types.CanDeEscalate | None = x_struct.EnumAttributeProperty(
        'CanDeescalate', enum_cls=pm_types.CanDeEscalate)
    _props = ('Source', 'CauseInfo', 'Kind', 'Priority', 'DefaultConditionGenerationDelay',
              'CanEscalate', 'CanDeescalate')
    _child_elements_order = (pm_qnames.Source,
                             pm_qnames.CauseInfo)


class LimitAlertConditionDescriptorContainer(AlertConditionDescriptorContainer):
    """Represents AlertConditionDescriptor in BICEPS."""

    NODETYPE = pm_qnames.LimitAlertConditionDescriptor
    STATE_QNAME = pm_qnames.LimitAlertConditionState
    MaxLimits: pm_types.Range = x_struct.SubElementProperty(pm_qnames.MaxLimits,
                                                            value_class=pm_types.Range,
                                                            default_py_value=pm_types.Range())
    AutoLimitSupported: bool = x_struct.BooleanAttributeProperty('AutoLimitSupported', implied_py_value=False)
    _props = ('MaxLimits', 'AutoLimitSupported')
    _child_elements_order = (pm_qnames.MaxLimits,)


class AlertSignalDescriptorContainer(AbstractAlertDescriptorContainer):
    """Represents AlertSignalDescriptor in BICEPS."""

    is_alert_signal_descriptor = True
    NODETYPE = pm_qnames.AlertSignalDescriptor
    STATE_QNAME = pm_qnames.AlertSignalState
    ConditionSignaled: str = x_struct.HandleRefAttributeProperty('ConditionSignaled')
    Manifestation: pm_types.AlertSignalManifestation = x_struct.EnumAttributeProperty('Manifestation',
                                                                                      enum_cls=pm_types.AlertSignalManifestation,
                                                                                      is_optional=False)
    Latching: bool = x_struct.BooleanAttributeProperty('Latching', default_py_value=False, is_optional=False)
    DefaultSignalGenerationDelay: DurationType = x_struct.DurationAttributeProperty(
        'DefaultSignalGenerationDelay',
        implied_py_value=0)
    MinSignalGenerationDelay: DurationType | None = x_struct.DurationAttributeProperty(
        'MinSignalGenerationDelay')
    MaxSignalGenerationDelay: DurationType | None = x_struct.DurationAttributeProperty(
        'MaxSignalGenerationDelay')
    SignalDelegationSupported: bool = x_struct.BooleanAttributeProperty('SignalDelegationSupported',
                                                                        implied_py_value=False)
    AcknowledgementSupported: bool = x_struct.BooleanAttributeProperty('AcknowledgementSupported',
                                                                       implied_py_value=False)
    AcknowledgeTimeout: DurationType | None = x_struct.DurationAttributeProperty('AcknowledgeTimeout')
    _props = ('ConditionSignaled', 'Manifestation', 'Latching', 'DefaultSignalGenerationDelay',
              'MinSignalGenerationDelay', 'MaxSignalGenerationDelay',
              'SignalDelegationSupported', 'AcknowledgementSupported', 'AcknowledgeTimeout')


class SystemContextDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    """Represents SystemContextDescriptor in BICEPS."""

    is_system_context_descriptor = True
    NODETYPE = pm_qnames.SystemContextDescriptor
    STATE_QNAME = pm_qnames.SystemContextState
    _child_elements_order = (pm_qnames.PatientContext,
                             pm_qnames.LocationContext,
                             pm_qnames.EnsembleContext,
                             pm_qnames.OperatorContext,
                             pm_qnames.WorkflowContext,
                             pm_qnames.MeansContext)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.PatientContext, (pm_qnames.PatientContextDescriptor,)),
        ChildDescriptorMapping(pm_qnames.LocationContext, (pm_qnames.LocationContextDescriptor,)),
        ChildDescriptorMapping(pm_qnames.EnsembleContext, (pm_qnames.EnsembleContextDescriptor,)),
        ChildDescriptorMapping(pm_qnames.OperatorContext, (pm_qnames.OperatorContextDescriptor,)),
        ChildDescriptorMapping(pm_qnames.WorkflowContext, (pm_qnames.WorkflowContextDescriptor,)),
        ChildDescriptorMapping(pm_qnames.MeansContext, (pm_qnames.MeansContextDescriptor,)),
    )


class AbstractContextDescriptorContainer(AbstractDescriptorContainer):
    """Represents AbstractContextDescriptor in BICEPS."""

    is_context_descriptor = True


class PatientContextDescriptorContainer(AbstractContextDescriptorContainer):
    """Represents PatientContextDescriptor in BICEPS."""

    NODETYPE = pm_qnames.PatientContextDescriptor
    STATE_QNAME = pm_qnames.PatientContextState


class LocationContextDescriptorContainer(AbstractContextDescriptorContainer):
    """Represents LocationContextDescriptor in BICEPS."""

    NODETYPE = pm_qnames.LocationContextDescriptor
    STATE_QNAME = pm_qnames.LocationContextState


class WorkflowContextDescriptorContainer(AbstractContextDescriptorContainer):
    """Represents WorkflowContextDescriptor in BICEPS."""

    NODETYPE = pm_qnames.WorkflowContextDescriptor
    STATE_QNAME = pm_qnames.WorkflowContextState


class OperatorContextDescriptorContainer(AbstractContextDescriptorContainer):
    """Represents OperatorContextDescriptor in BICEPS."""

    NODETYPE = pm_qnames.OperatorContextDescriptor
    STATE_QNAME = pm_qnames.OperatorContextState


class MeansContextDescriptorContainer(AbstractContextDescriptorContainer):
    """Represents MeansContextDescriptor in BICEPS."""

    NODETYPE = pm_qnames.MeansContextDescriptor
    STATE_QNAME = pm_qnames.MeansContextState


class EnsembleContextDescriptorContainer(AbstractContextDescriptorContainer):
    """Represents EnsembleContextDescriptor in BICEPS."""

    NODETYPE = pm_qnames.EnsembleContextDescriptor
    STATE_QNAME = pm_qnames.EnsembleContextState


_classes = inspect.getmembers(sys.modules[__name__],
                              lambda member: inspect.isclass(member) and member.__module__ == __name__)
_classes_with_nodetype = [c[1] for c in _classes if hasattr(c[1], 'NODETYPE') and c[1].NODETYPE is not None]
# make a dictionary from found classes: (Key is NODETYPE, value is the class itself

_name_class_lookup = {c.NODETYPE: c for c in _classes_with_nodetype}

_name_class_xtra_lookup = {
    pm_qnames.Battery: BatteryDescriptorContainer,
    pm_qnames.Mds: MdsDescriptorContainer,
    pm_qnames.Vmd: VmdDescriptorContainer,
    pm_qnames.Sco: ScoDescriptorContainer,
    pm_qnames.Channel: ChannelDescriptorContainer,
    pm_qnames.Clock: ClockDescriptorContainer,
    pm_qnames.SystemContext: SystemContextDescriptorContainer,
    pm_qnames.PatientContext: PatientContextDescriptorContainer,
    pm_qnames.LocationContext: LocationContextDescriptorContainer,
    pm_qnames.WorkflowContext: WorkflowContextDescriptorContainer,
    pm_qnames.OperatorContext: OperatorContextDescriptorContainer,
    pm_qnames.MeansContext: MeansContextDescriptorContainer,
    pm_qnames.EnsembleContext: EnsembleContextDescriptorContainer,
    pm_qnames.AlertSystem: AlertSystemDescriptorContainer,
    pm_qnames.AlertCondition: AlertConditionDescriptorContainer,
    pm_qnames.AlertSignal: AlertSignalDescriptorContainer,
}
_name_class_lookup.update(_name_class_xtra_lookup)


def get_container_class(qname: etree_.QName) -> type[AbstractDescriptorContainer]:
    """:param qname: a QName instance"""
    return _name_class_lookup.get(qname)
