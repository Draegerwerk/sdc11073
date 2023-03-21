import inspect
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Tuple

from lxml import etree as etree_

from .containerbase import ContainerBase
from .. import observableproperties as properties
from ..namespaces import NamespaceHelper
from ..xml_types import ext_qnames as ext
from ..xml_types import msg_qnames as msg
from ..xml_types import pm_qnames
from ..xml_types import pm_types
from ..xml_types import xml_structure as cp


@dataclass(frozen=True)
class ChildDescriptorMapping:
    """Maps element names to node types. Needed when building a xml tree of descriptors.
    The name of a child element is often not identical to the type of the descriptor, e.g. a channel uses
    pm.Metric for all classes derived from AbstractMetricDescriptor. """
    child_qname: etree_.QName
    node_types: Tuple[etree_.QName, ...] = None

    def __repr__(self):
        if self.node_types is None:
            return f'{self.__class__.__name__} name={self.child_qname.localname} (no types)'
        else:
            types = ', '.join([t.localname for t in self.node_types])
            return f'{self.__class__.__name__} name={self.child_qname.localname} types={types}'


def sorted_child_data(obj, member_name):
    """
    :return: an iterator with whatever the members have, starting with base class members
    """
    classes = inspect.getmro(obj.__class__)
    for cls in reversed(classes):
        try:
            names = cls.__dict__[member_name]  # only access class member of this class, not parent
            for name in names:
                yield name
        except KeyError:
            continue


def make_descriptor_node(descriptor_container, tag: etree_.QName, ns_helper: NamespaceHelper, set_xsi_type: bool = True,
                         connect_child_descriptors: bool = False):
    """
    Creates a lxml etree node from instance data.
    :param descriptor_container: a descriptor container instance
    :param ns_helper:  namespaces.NamespaceHelper instance
    :param set_xsi_type: if true, the NODETYPE will be used to set the xsi:type attribute of the node
    :param tag: tag of node
    :param connect_child_descriptors: if True, the whole subtree is included
    :return: an etree node
    """
    if set_xsi_type:
        ns_map = ns_helper.partial_map(ns_helper.PM,
                                       ns_helper.XSI, )

    else:
        ns_map = ns_helper.partial_map(ns_helper.PM)
    node = etree_.Element(tag,
                          attrib={'Handle': descriptor_container.Handle},
                          nsmap=ns_map)
    descriptor_container.update_node(node, ns_helper, set_xsi_type)  # create all
    if connect_child_descriptors:
        # append all child containers, then bring all child elements in correct order
        for node_type, child_list in descriptor_container.child_containers_by_type.items():
            child_tag, set_xsi = descriptor_container.tag_name_for_child_descriptor(node_type)
            for child in child_list:
                child_node = make_descriptor_node(child, child_tag, ns_helper, set_xsi, connect_child_descriptors=True)
                node.append(child_node)
    descriptor_container.sort_child_nodes(node)
    return node


class AbstractDescriptorContainer(ContainerBase):
    """
    This class represents the AbstractDescriptor
    """
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

    Handle = cp.HandleAttributeProperty('Handle', is_optional=False)
    Extension = cp.ExtensionNodeProperty(ext.Extension)
    DescriptorVersion = cp.VersionCounterAttributeProperty('DescriptorVersion',
                                                           default_py_value=0)
    SafetyClassification = cp.EnumAttributeProperty('SafetyClassification',
                                                    implied_py_value=pm_types.SafetyClassification.INF,
                                                    enum_cls=pm_types.SafetyClassification)
    Type = cp.SubElementProperty(pm_qnames.Type, value_class=pm_types.CodedValue, is_optional=True)
    # pylint: enable=invalid-name
    _props = ('Handle', 'DescriptorVersion', 'SafetyClassification', 'Extension', 'Type')
    _child_elements_order = (ext.Extension, pm_qnames.Type)  # child elements in BICEPS order
    STATE_QNAME = None
    extension_class_lookup = {msg.Retrievability: pm_types.Retrievability}

    def __init__(self, handle, parent_handle):
        super().__init__()
        self._parent_handle = parent_handle
        self.Handle = handle
        self.child_containers_by_type = defaultdict(list)
        self._source_mds = None  # needed on device side if mdib contains > 1 mds

    @property
    def coding(self):
        return self.Type.coding if self.Type is not None else None

    @property
    def code_id(self):
        return self.Type.Code if self.Type is not None else None  # pylint:disable=no-member

    @property
    def coding_system(self):
        return self.Type.CodingSystem if self.Type is not None else None  # pylint:disable=no-member

    @property
    def parent_handle(self):
        return self._parent_handle

    @parent_handle.setter
    def parent_handle(self, value):
        self._parent_handle = value

    @property
    def retrievability(self) -> [pm_types.Retrievability, None]:
        if self.Extension is None:
            return None
        return self.Extension.value.get(msg.Retrievability)

    @retrievability.setter
    def retrievability(self, retrievability_instance: pm_types.Retrievability):
        value = self.Extension.value
        value[msg.Retrievability] = retrievability_instance

    def increment_descriptor_version(self):
        # pylint: disable=invalid-name
        if self.DescriptorVersion is None:
            self.DescriptorVersion = 1
        else:
            self.DescriptorVersion += 1
        # pylint: enable=invalid-name

    def update_from_other_container(self, other, skipped_properties=None):
        if other.Handle != self.Handle:
            raise ValueError(
                f'Update from a container with different handle is not possible! '
                f'Have "{self.Handle}", got "{other.Handle}"')
        self._update_from_other(other, skipped_properties)

    def add_child(self, child_descriptor_container):
        if self.is_leaf:
            raise ValueError(f'{self.__class__.__name__} can not have children')
        self.child_containers_by_type[child_descriptor_container.NODETYPE].append(child_descriptor_container)

    def rm_child(self, child_descriptor_container):
        if self.is_leaf:
            raise ValueError(f'{self.__class__.__name__} does not have children')
        tag_specific_list = self.child_containers_by_type[child_descriptor_container.NODETYPE]
        for container in tag_specific_list:
            if container.Handle == child_descriptor_container.Handle:
                tag_specific_list.remove(container)

    def get_actual_value(self, attr_name):
        """ ignores default value and implied value, e.g. returns None if value is not present in xml"""
        return getattr(self.__class__, attr_name).get_actual_value(self)

    def diff(self, other, ignore_property_names: Optional[List[str]] = None):
        """
        Compare with another descriptor. It compares all properties plus the parent handle member.
        :param other: the object (descriptor container) to compare with
        :param ignore_property_names: list of properties that shall be excluded from diff calculation
        :return: textual representation of differences or None if equal
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

    def mk_descriptor_node(self, tag, nsmapper, set_xsi_type=True, connect_child_descriptors=False):
        """
        Creates a lxml etree node from instance data.
        :param tag: tag of node
        :param nsmapper: namespaces.DocNamespaceHelper instance
        :param set_xsi_type: if True, adds Type attribute to node
        :param connect_child_descriptors: if True, the whole subtree is included
        :return: an etree node
        """
        return make_descriptor_node(self, tag, nsmapper, set_xsi_type, connect_child_descriptors)

    def tag_name_for_child_descriptor(self, node_type: etree_.QName):
        """This method determines the tag name of a child descriptor (needed when the xml tree of the
        descriptor is created). It uses the _child_elements_order members of the class itself and its base classes
        which map node type to tag name.
        :param node_type: the type QName (NODETYPE member)
        :return: tuple(QName, set_xsi_type_flag)"""
        for child in sorted_child_data(self, '_child_descriptor_name_mappings'):
            if child.node_types is not None and node_type in child.node_types:
                set_xsi_type = len(child.node_types) > 1
                return child.child_qname, set_xsi_type
        raise ValueError(f'{node_type} not known in child declarations of {self.__class__.__name__}')

    def sort_child_nodes(self, node: etree_.Element) -> None:
        """ Brings all child elements of node in correct order (BICEPS schema).
        raises a ValueError if a child node exist that is not listed in ordered_tags
        :param node: the element to be sorted
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
        self._source_mds = handle

    @property
    def source_mds(self):
        return self._source_mds

    def __str__(self):
        name = self.NODETYPE.localname or None
        return f'Descriptor "{name}": handle={self.Handle} descriptor version={self.DescriptorVersion} ' \
               f'parent handle={self.parent_handle}'

    def __repr__(self):
        name = self.NODETYPE.localname or None
        return f'Descriptor "{name}": handle={self.Handle} descriptor version={self.DescriptorVersion} ' \
               f'parent={self.parent_handle}'

    @classmethod
    def from_node(cls, node, parent_handle=None):
        obj = cls(handle=None,  # will be determined in constructor from node value
                  parent_handle=parent_handle)
        obj.update_from_node(node)
        return obj


class AbstractDeviceComponentDescriptorContainer(AbstractDescriptorContainer):
    is_component_descriptor = True
    is_leaf = False
    ProductionSpecification = cp.SubElementListProperty(pm_qnames.ProductionSpecification,
                                                        value_class=pm_types.ProductionSpecification)
    _props = ('ProductionSpecification',)
    _child_elements_order = (pm_qnames.ProductionSpecification,)


class AbstractComplexDeviceComponentDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    _child_elements_order = (pm_qnames.AlertSystem, pm_qnames.Sco)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.AlertSystem, (pm_qnames.AlertSystemDescriptor,)),
        ChildDescriptorMapping(pm_qnames.Sco, (pm_qnames.ScoDescriptor,)))


class MdsDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    NODETYPE = pm_qnames.MdsDescriptor
    STATE_QNAME = pm_qnames.MdsState
    # pylint: disable=invalid-name
    MetaData = cp.SubElementProperty(pm_qnames.MetaData, value_class=pm_types.MetaData, is_optional=True)
    ApprovedJurisdictions = cp.SubElementProperty(pm_qnames.ApprovedJurisdictions,
                                                  value_class=pm_types.ApprovedJurisdictions,
                                                  is_optional=True)
    # pylint: enable=invalid-name
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
    NODETYPE = pm_qnames.VmdDescriptor
    STATE_QNAME = pm_qnames.VmdState

    ApprovedJurisdictions = cp.SubElementProperty(pm_qnames.ApprovedJurisdictions,
                                                  value_class=pm_types.ApprovedJurisdictions,
                                                  is_optional=True)
    _props = ('ApprovedJurisdictions',)
    _child_elements_order = (pm_qnames.ApprovedJurisdictions,
                             pm_qnames.Channel,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.Channel, (pm_qnames.ChannelDescriptor,)),
    )


class ChannelDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm_qnames.ChannelDescriptor
    STATE_QNAME = pm_qnames.ChannelState
    _child_elements_order = (pm_qnames.Metric,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.Metric, (pm_qnames.NumericMetricDescriptor,
                                                  pm_qnames.StringMetricDescriptor,
                                                  pm_qnames.EnumStringMetricDescriptor,
                                                  pm_qnames.RealTimeSampleArrayMetricDescriptor,
                                                  pm_qnames.DistributionSampleArrayMetricDescriptor,
                                                  )
                               ),
    )


class ClockDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm_qnames.ClockDescriptor
    STATE_QNAME = pm_qnames.ClockState
    # pylint: disable=invalid-name
    TimeProtocol = cp.SubElementListProperty(pm_qnames.TimeProtocol, value_class=pm_types.CodedValue)
    Resolution = cp.DurationAttributeProperty('Resolution')  # optional,  xsd:duration
    # pylint: enable=invalid-name
    _props = ('TimeProtocol', 'Resolution')
    _child_elements_order = (pm_qnames.TimeProtocol,)


class BatteryDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm_qnames.BatteryDescriptor
    STATE_QNAME = pm_qnames.BatteryState
    # pylint: disable=invalid-name
    CapacityFullCharge = cp.SubElementProperty(pm_qnames.CapacityFullCharge,
                                               value_class=pm_types.Measurement,
                                               is_optional=True)
    CapacitySpecified = cp.SubElementProperty(pm_qnames.CapacitySpecified,
                                              value_class=pm_types.Measurement,
                                              is_optional=True)
    VoltageSpecified = cp.SubElementProperty(pm_qnames.VoltageSpecified,
                                             value_class=pm_types.Measurement,
                                             is_optional=True)
    # pylint: enable=invalid-name
    _props = ('CapacityFullCharge', 'CapacitySpecified', 'VoltageSpecified')
    _child_elements_order = (pm_qnames.CapacityFullCharge,
                             pm_qnames.CapacitySpecified,
                             pm_qnames.VoltageSpecified)


class ScoDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
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
                                                     pm_qnames.ActivateOperationDescriptor
                                                     )
                               ),
    )


class AbstractMetricDescriptorContainer(AbstractDescriptorContainer):
    is_metric_descriptor = True
    Unit = cp.SubElementProperty(pm_qnames.Unit, value_class=pm_types.CodedValue)
    BodySite = cp.SubElementListProperty(pm_qnames.BodySite, value_class=pm_types.CodedValue)
    Relation = cp.SubElementListProperty(pm_qnames.Relation, value_class=pm_types.Relation)  # o...n
    MetricCategory = cp.EnumAttributeProperty('MetricCategory',
                                              enum_cls=pm_types.MetricCategory,
                                              default_py_value=pm_types.MetricCategory.UNSPECIFIED)  # required
    DerivationMethod = cp.EnumAttributeProperty('DerivationMethod', enum_cls=pm_types.DerivationMethod)  # optional
    #  There is an implied value defined, but it is complicated, therefore here not implemented:
    # - If pm:AbstractDescriptor/@MetricCategory is "Set" or "Preset", then the default value of DerivationMethod is "Man"
    # - If pm:AbstractDescriptor/@MetricCategory is "Clc", "Msrmt", "Rcmm", then the default value of DerivationMethod is "Auto"
    # - If pm:AbstractDescriptor/@MetricCategory is "Unspec", then no default value is being implied</xsd:documentation>
    MetricAvailability = cp.EnumAttributeProperty('MetricAvailability',
                                                  enum_cls=pm_types.MetricAvailability,
                                                  default_py_value=pm_types.MetricAvailability.CONTINUOUS)  # required
    MaxMeasurementTime = cp.DurationAttributeProperty('MaxMeasurementTime')  # optional,  xsd:duration
    MaxDelayTime = cp.DurationAttributeProperty('MaxDelayTime')  # optional,  xsd:duration
    DeterminationPeriod = cp.DurationAttributeProperty('DeterminationPeriod')  # optional,  xsd:duration
    LifeTimePeriod = cp.DurationAttributeProperty('LifeTimePeriod')  # optional,  xsd:duration
    ActivationDuration = cp.DurationAttributeProperty('ActivationDuration')  # optional,  xsd:duration
    _props = (
        'Unit', 'BodySite', 'Relation', 'MetricCategory', 'DerivationMethod', 'MetricAvailability',
        'MaxMeasurementTime',
        'MaxDelayTime', 'DeterminationPeriod', 'LifeTimePeriod', 'ActivationDuration')
    _child_elements_order = (pm_qnames.Unit,
                             pm_qnames.BodySite,
                             pm_qnames.Relation)


class NumericMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = pm_qnames.NumericMetricDescriptor
    STATE_QNAME = pm_qnames.NumericMetricState
    TechnicalRange = cp.SubElementListProperty(pm_qnames.TechnicalRange, value_class=pm_types.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    AveragingPeriod = cp.DurationAttributeProperty('AveragingPeriod')  # optional
    _props = ('TechnicalRange', 'Resolution', 'AveragingPeriod')
    _child_elements_order = (pm_qnames.TechnicalRange,)


class StringMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = pm_qnames.StringMetricDescriptor
    STATE_QNAME = pm_qnames.StringMetricState


class EnumStringMetricDescriptorContainer(StringMetricDescriptorContainer):
    NODETYPE = pm_qnames.EnumStringMetricDescriptor
    STATE_QNAME = pm_qnames.EnumStringMetricState
    AllowedValue = cp.SubElementListProperty(pm_qnames.AllowedValue, value_class=pm_types.AllowedValue)
    _props = ('AllowedValue',)
    _child_elements_order = (pm_qnames.AllowedValue,)


class RealTimeSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    is_realtime_sample_array_metric_descriptor = True
    NODETYPE = pm_qnames.RealTimeSampleArrayMetricDescriptor
    STATE_QNAME = pm_qnames.RealTimeSampleArrayMetricState
    TechnicalRange = cp.SubElementListProperty(pm_qnames.TechnicalRange, value_class=pm_types.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    SamplePeriod = cp.DurationAttributeProperty('SamplePeriod', is_optional=False)
    _props = ('TechnicalRange', 'Resolution', 'SamplePeriod')
    _child_elements_order = (pm_qnames.TechnicalRange,)


class DistributionSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = pm_qnames.DistributionSampleArrayMetricDescriptor
    STATE_QNAME = pm_qnames.DistributionSampleArrayMetricState
    TechnicalRange = cp.SubElementListProperty(pm_qnames.TechnicalRange, value_class=pm_types.Range)
    DomainUnit = cp.SubElementProperty(pm_qnames.DomainUnit, value_class=pm_types.CodedValue)
    DistributionRange = cp.SubElementProperty(pm_qnames.DistributionRange, value_class=pm_types.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    _props = ('TechnicalRange', 'DomainUnit', 'DistributionRange', 'Resolution')
    _child_elements_order = (pm_qnames.TechnicalRange,
                             pm_qnames.DomainUnit,
                             pm_qnames.DistributionRange)


class AbstractOperationDescriptorContainer(AbstractDescriptorContainer):
    is_operational_descriptor = True
    OperationTarget = cp.HandleRefAttributeProperty('OperationTarget', is_optional=False)
    MaxTimeToFinish = cp.DurationAttributeProperty('MaxTimeToFinish')  # optional  xsd:duration
    InvocationEffectiveTimeout = cp.DurationAttributeProperty('InvocationEffectiveTimeout')  # optional  xsd:duration
    Retriggerable = cp.BooleanAttributeProperty('Retriggerable', implied_py_value=True)  # optional
    AccessLevel = cp.EnumAttributeProperty('AccessLevel', implied_py_value=pm_types.T_AccessLevel.USER,
                                           enum_cls=pm_types.T_AccessLevel)
    _props = ('OperationTarget', 'MaxTimeToFinish', 'InvocationEffectiveTimeout', 'Retriggerable', 'AccessLevel')


class SetValueOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    NODETYPE = pm_qnames.SetValueOperationDescriptor
    STATE_QNAME = pm_qnames.SetValueOperationState


class SetStringOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    NODETYPE = pm_qnames.SetStringOperationDescriptor
    STATE_QNAME = pm_qnames.SetStringOperationState
    MaxLength = cp.IntegerAttributeProperty('MaxLength')
    _props = ('MaxLength',)


class AbstractSetStateOperationDescriptor(AbstractOperationDescriptorContainer):
    ModifiableData = cp.SubElementStringListProperty(pm_qnames.ModifiableData)
    _props = ('ModifiableData',)
    _child_elements_order = (pm_qnames.ModifiableData,)


class SetContextStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm_qnames.SetContextStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetContextStateOperationState


class SetMetricStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm_qnames.SetMetricStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetMetricStateOperationState


class SetComponentStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm_qnames.SetComponentStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetComponentStateOperationState


class SetAlertStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm_qnames.SetAlertStateOperationDescriptor
    STATE_QNAME = pm_qnames.SetAlertStateOperationState


class ActivateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm_qnames.ActivateOperationDescriptor
    STATE_QNAME = pm_qnames.ActivateOperationState
    Argument = cp.SubElementListProperty(pm_qnames.Argument, value_class=pm_types.ActivateOperationDescriptorArgument)
    _props = ('Argument',)
    _child_elements_order = (pm_qnames.Argument,)


class AbstractAlertDescriptorContainer(AbstractDescriptorContainer):
    """AbstractAlertDescriptor acts as a base class for all alert descriptors that contain static alert meta information.
     This class has nor specific data."""
    is_alert_descriptor = True
    is_leaf = False


class AlertSystemDescriptorContainer(AbstractAlertDescriptorContainer):
    """AlertSystemDescriptor describes an ALERT SYSTEM to detect ALERT CONDITIONs and generate ALERT SIGNALs,
    which belong to specific ALERT CONDITIONs.
    ALERT CONDITIONs are represented by a list of pm:AlertConditionDescriptor ELEMENTs and
    ALERT SIGNALs are represented by a list of pm:AlertSignalDescriptor ELEMENTs.
    """
    NODETYPE = pm_qnames.AlertSystemDescriptor
    STATE_QNAME = pm_qnames.AlertSystemState
    MaxPhysiologicalParallelAlarms = cp.UnsignedIntAttributeProperty('MaxPhysiologicalParallelAlarms')
    MaxTechnicalParallelAlarms = cp.UnsignedIntAttributeProperty('MaxTechnicalParallelAlarms')
    SelfCheckPeriod = cp.DurationAttributeProperty('SelfCheckPeriod')
    _props = ('MaxPhysiologicalParallelAlarms', 'MaxTechnicalParallelAlarms', 'SelfCheckPeriod')
    _child_elements_order = (pm_qnames.AlertCondition,
                             pm_qnames.AlertSignal)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm_qnames.AlertCondition, (pm_qnames.AlertConditionDescriptor,
                                                          pm_qnames.LimitAlertConditionDescriptor
                                                          )
                               ),
        ChildDescriptorMapping(pm_qnames.AlertSignal, (pm_qnames.AlertSignalDescriptor,)),
    )


class AlertConditionDescriptorContainer(AbstractAlertDescriptorContainer):
    """An ALERT CONDITION contains the information about a potentially or actually HAZARDOUS SITUATION.
      Examples: a physiological alarm limit has been exceeded or a sensor has been unplugged."""
    is_alert_condition_descriptor = True
    NODETYPE = pm_qnames.AlertConditionDescriptor
    STATE_QNAME = pm_qnames.AlertConditionState
    Source = cp.SubElementHandleRefListProperty(pm_qnames.Source)  # a list of 0...n pm:HandleRef elements
    CauseInfo = cp.SubElementListProperty(pm_qnames.CauseInfo, value_class=pm_types.CauseInfo)
    Kind = cp.EnumAttributeProperty('Kind', default_py_value=pm_types.AlertConditionKind.OTHER,
                                    enum_cls=pm_types.AlertConditionKind, is_optional=False)
    Priority = cp.EnumAttributeProperty('Priority', default_py_value=pm_types.AlertConditionPriority.NONE,
                                        enum_cls=pm_types.AlertConditionPriority, is_optional=False)
    DefaultConditionGenerationDelay = cp.DurationAttributeProperty('DefaultConditionGenerationDelay',
                                                                   implied_py_value=0)
    CanEscalate = cp.EnumAttributeProperty('CanEscalate', enum_cls=pm_types.CanEscalateAlertConditionPriority)
    CanDeescalate = cp.EnumAttributeProperty('CanDeescalate', enum_cls=pm_types.CanDeEscalateAlertConditionPriority)
    _props = ('Source', 'CauseInfo', 'Kind', 'Priority', 'DefaultConditionGenerationDelay',
              'CanEscalate', 'CanDeescalate')
    _child_elements_order = (pm_qnames.Source,
                             pm_qnames.CauseInfo)


class LimitAlertConditionDescriptorContainer(AlertConditionDescriptorContainer):
    NODETYPE = pm_qnames.LimitAlertConditionDescriptor
    STATE_QNAME = pm_qnames.LimitAlertConditionState
    MaxLimits = cp.SubElementProperty(pm_qnames.MaxLimits, value_class=pm_types.Range,
                                      default_py_value=pm_types.Range())
    AutoLimitSupported = cp.BooleanAttributeProperty('AutoLimitSupported', implied_py_value=False)
    _props = ('MaxLimits', 'AutoLimitSupported',)
    _child_elements_order = (pm_qnames.MaxLimits,)


class AlertSignalDescriptorContainer(AbstractAlertDescriptorContainer):
    is_alert_signal_descriptor = True
    NODETYPE = pm_qnames.AlertSignalDescriptor
    STATE_QNAME = pm_qnames.AlertSignalState
    ConditionSignaled = cp.HandleRefAttributeProperty('ConditionSignaled')
    Manifestation = cp.EnumAttributeProperty('Manifestation', enum_cls=pm_types.AlertSignalManifestation,
                                             is_optional=False)
    Latching = cp.BooleanAttributeProperty('Latching', default_py_value=False, is_optional=False)
    DefaultSignalGenerationDelay = cp.DurationAttributeProperty('DefaultSignalGenerationDelay', implied_py_value=0)
    MinSignalGenerationDelay = cp.DurationAttributeProperty('MinSignalGenerationDelay')
    MaxSignalGenerationDelay = cp.DurationAttributeProperty('MaxSignalGenerationDelay')
    SignalDelegationSupported = cp.BooleanAttributeProperty('SignalDelegationSupported', implied_py_value=False)
    AcknowledgementSupported = cp.BooleanAttributeProperty('AcknowledgementSupported', implied_py_value=False)
    AcknowledgeTimeout = cp.DurationAttributeProperty('AcknowledgeTimeout')  # optional
    _props = ('ConditionSignaled', 'Manifestation', 'Latching', 'DefaultSignalGenerationDelay',
              'MinSignalGenerationDelay', 'MaxSignalGenerationDelay',
              'SignalDelegationSupported', 'AcknowledgementSupported', 'AcknowledgeTimeout')


class SystemContextDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
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
    is_context_descriptor = True


class PatientContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm_qnames.PatientContextDescriptor
    STATE_QNAME = pm_qnames.PatientContextState


class LocationContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm_qnames.LocationContextDescriptor
    STATE_QNAME = pm_qnames.LocationContextState


class WorkflowContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm_qnames.WorkflowContextDescriptor
    STATE_QNAME = pm_qnames.WorkflowContextState


class OperatorContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm_qnames.OperatorContextDescriptor
    STATE_QNAME = pm_qnames.OperatorContextState


class MeansContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm_qnames.MeansContextDescriptor
    STATE_QNAME = pm_qnames.MeansContextState


class EnsembleContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm_qnames.EnsembleContextDescriptor
    STATE_QNAME = pm_qnames.EnsembleContextState


_classes = inspect.getmembers(sys.modules[__name__],
                              lambda member: inspect.isclass(member) and member.__module__ == __name__)
_classes_with_NODETYPE = [c[1] for c in _classes if hasattr(c[1], 'NODETYPE') and c[1].NODETYPE is not None]
# make a dictionary from found classes: (Key is NODETYPE, value is the class itself

_name_class_lookup = {c.NODETYPE: c for c in _classes_with_NODETYPE}

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


def get_container_class(qname):
    """
    :param qname: a QName instance
    """
    return _name_class_lookup.get(qname)
