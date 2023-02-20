import inspect
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Tuple

from lxml import etree as etree_

from . import containerproperties as cp
from .containerbase import ContainerBase
from .. import ext_qnames as ext
from .. import msg_qnames as msg
from .. import observableproperties as properties
from .. import pm_qnames as pm
from .. import pmtypes
from ..namespaces import NamespaceHelper



@dataclass(frozen=True)
class ChildDescriptorMapping:
    """Maps element names to node types. Needed when building a xml tree of descriptors.
    The name of a child element is often not identical to the type of the descriptor, e.g. a channel uses
    pm.Metric for all classes derived from AbstractMetricDescriptor. """
    child_qname: etree_.QName
    node_types: Tuple[etree_.QName] = None

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


def make_descriptor_node(descriptor_container, tag: etree_.QName, ns_helper: NamespaceHelper, set_xsi_type: bool=True, connect_child_descriptors: bool =False):
    """
    Creates a lxml etree node from instance data.
    :param descriptor_container: a descriptor container instance
    :param ns_helper:  namespaces.NamespaceHelper instance
    :param set_xsi_type: if true, the NODETYPE will be used to set the xsi:type attribute of the node
    :param tag: tag of node
    :param connect_child_descriptors: if True, the whole sub-tree is included
    :return: an etree node
    """
    if set_xsi_type:
        ns_map = ns_helper.partial_map(ns_helper.PM,
                                       ns_helper.XSI,)

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
                                                    implied_py_value=pmtypes.SafetyClassification.INF,
                                                    enum_cls=pmtypes.SafetyClassification)
    Type = cp.SubElementProperty(pm.Type, value_class=pmtypes.CodedValue, is_optional=True)
    # pylint: enable=invalid-name
    _props = ('Handle', 'DescriptorVersion', 'SafetyClassification', 'Extension', 'Type')
    _child_elements_order = (ext.Extension, pm.Type)  # child elements in BICEPS order
    STATE_QNAME = None
    extension_class_lookup = {msg.Retrievability: pmtypes.Retrievability}

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
    def retrievability(self) -> [pmtypes.Retrievability, None]:
        if self.Extension is None:
            return None
        return self.Extension.value.get(msg.Retrievability)

    @retrievability.setter
    def retrievability(self, retrievability_instance: pmtypes.Retrievability):
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
    ProductionSpecification = cp.SubElementListProperty(pm.ProductionSpecification,
                                                        value_class=pmtypes.ProductionSpecification)
    _props = ('ProductionSpecification',)
    _child_elements_order = (pm.ProductionSpecification,)


class AbstractComplexDeviceComponentDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    _child_elements_order = (pm.AlertSystem, pm.Sco)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.AlertSystem, (pm.AlertSystemDescriptor,)),
        ChildDescriptorMapping(pm.Sco, (pm.ScoDescriptor,)))


class MdsDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    NODETYPE = pm.MdsDescriptor
    STATE_QNAME = pm.MdsState
    # pylint: disable=invalid-name
    MetaData = cp.SubElementProperty(pm.MetaData, value_class=pmtypes.MetaData, is_optional=True)
    ApprovedJurisdictions = cp.SubElementProperty(pm.ApprovedJurisdictions,
                                                  value_class=pmtypes.ApprovedJurisdictions,
                                                  is_optional=True)
    # pylint: enable=invalid-name
    _props = ('MetaData', 'ApprovedJurisdictions')
    _child_elements_order = (pm.MetaData,
                             pm.SystemContext,
                             pm.Clock,
                             pm.Battery,
                             pm.ApprovedJurisdictions,
                             pm.Vmd)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.SystemContext, (pm.SystemContextDescriptor,)),
        ChildDescriptorMapping(pm.Clock, (pm.ClockDescriptor,)),
        ChildDescriptorMapping(pm.Battery, (pm.BatteryDescriptor,)),
        ChildDescriptorMapping(pm.Vmd, (pm.VmdDescriptor,)),
    )


class VmdDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    NODETYPE = pm.VmdDescriptor
    STATE_QNAME = pm.VmdState

    ApprovedJurisdictions = cp.SubElementProperty(pm.ApprovedJurisdictions,
                                                  value_class=pmtypes.ApprovedJurisdictions,
                                                  is_optional=True)
    _props = ('ApprovedJurisdictions',)
    _child_elements_order = (pm.ApprovedJurisdictions,
                             pm.Channel,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.Channel, (pm.ChannelDescriptor,)),
    )


class ChannelDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm.ChannelDescriptor
    STATE_QNAME = pm.ChannelState
    _child_elements_order = (pm.Metric,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.Metric, (pm.NumericMetricDescriptor,
                                           pm.StringMetricDescriptor,
                                           pm.EnumStringMetricDescriptor,
                                           pm.RealTimeSampleArrayMetricDescriptor,
                                           pm.DistributionSampleArrayMetricDescriptor,
                                           )
                               ),
    )


class ClockDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm.ClockDescriptor
    STATE_QNAME = pm.ClockState
    # pylint: disable=invalid-name
    TimeProtocol = cp.SubElementListProperty(pm.TimeProtocol, value_class=pmtypes.CodedValue)
    Resolution = cp.DurationAttributeProperty('Resolution')  # optional,  xsd:duration
    # pylint: enable=invalid-name
    _props = ('TimeProtocol', 'Resolution')
    _child_elements_order = (pm.TimeProtocol,)


class BatteryDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm.BatteryDescriptor
    STATE_QNAME = pm.BatteryState
    # pylint: disable=invalid-name
    CapacityFullCharge = cp.SubElementProperty(pm.CapacityFullCharge,
                                               value_class=pmtypes.Measurement,
                                               is_optional=True)
    CapacitySpecified = cp.SubElementProperty(pm.CapacitySpecified,
                                              value_class=pmtypes.Measurement,
                                              is_optional=True)
    VoltageSpecified = cp.SubElementProperty(pm.VoltageSpecified,
                                             value_class=pmtypes.Measurement,
                                             is_optional=True)
    # pylint: enable=invalid-name
    _props = ('CapacityFullCharge', 'CapacitySpecified', 'VoltageSpecified')
    _child_elements_order = (pm.CapacityFullCharge,
                             pm.CapacitySpecified,
                             pm.VoltageSpecified)


class ScoDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = pm.ScoDescriptor
    STATE_QNAME = pm.ScoState
    _child_elements_order = (pm.Operation,)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.Operation, (pm.SetValueOperationDescriptor,
                                              pm.SetStringOperationDescriptor,
                                              pm.SetContextStateOperationDescriptor,
                                              pm.SetMetricStateOperationDescriptor,
                                              pm.SetComponentStateOperationDescriptor,
                                              pm.SetAlertStateOperationDescriptor,
                                              pm.ActivateOperationDescriptor
                                              )
                               ),
    )


class AbstractMetricDescriptorContainer(AbstractDescriptorContainer):
    is_metric_descriptor = True
    Unit = cp.SubElementProperty(pm.Unit, value_class=pmtypes.CodedValue)
    BodySite = cp.SubElementListProperty(pm.BodySite, value_class=pmtypes.CodedValue)
    Relation = cp.SubElementListProperty(pm.Relation, value_class=pmtypes.Relation)  # o...n
    MetricCategory = cp.EnumAttributeProperty('MetricCategory',
                                              enum_cls=pmtypes.MetricCategory,
                                              default_py_value=pmtypes.MetricCategory.UNSPECIFIED)  # required
    DerivationMethod = cp.EnumAttributeProperty('DerivationMethod', enum_cls=pmtypes.DerivationMethod)  # optional
    #  There is an implied value defined, but it is complicated, therefore here not implemented:
    # - If pm:AbstractDescriptor/@MetricCategory is "Set" or "Preset", then the default value of DerivationMethod is "Man"
    # - If pm:AbstractDescriptor/@MetricCategory is "Clc", "Msrmt", "Rcmm", then the default value of DerivationMethod is "Auto"
    # - If pm:AbstractDescriptor/@MetricCategory is "Unspec", then no default value is being implied</xsd:documentation>
    MetricAvailability = cp.EnumAttributeProperty('MetricAvailability',
                                                  enum_cls=pmtypes.MetricAvailability,
                                                  default_py_value=pmtypes.MetricAvailability.CONTINUOUS)  # required
    MaxMeasurementTime = cp.DurationAttributeProperty('MaxMeasurementTime')  # optional,  xsd:duration
    MaxDelayTime = cp.DurationAttributeProperty('MaxDelayTime')  # optional,  xsd:duration
    DeterminationPeriod = cp.DurationAttributeProperty('DeterminationPeriod')  # optional,  xsd:duration
    LifeTimePeriod = cp.DurationAttributeProperty('LifeTimePeriod')  # optional,  xsd:duration
    ActivationDuration = cp.DurationAttributeProperty('ActivationDuration')  # optional,  xsd:duration
    _props = (
        'Unit', 'BodySite', 'Relation', 'MetricCategory', 'DerivationMethod', 'MetricAvailability',
        'MaxMeasurementTime',
        'MaxDelayTime', 'DeterminationPeriod', 'LifeTimePeriod', 'ActivationDuration')
    _child_elements_order = (pm.Unit,
                             pm.BodySite,
                             pm.Relation)


class NumericMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = pm.NumericMetricDescriptor
    STATE_QNAME = pm.NumericMetricState
    TechnicalRange = cp.SubElementListProperty(pm.TechnicalRange, value_class=pmtypes.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    AveragingPeriod = cp.DurationAttributeProperty('AveragingPeriod')  # optional
    _props = ('TechnicalRange', 'Resolution', 'AveragingPeriod')
    _child_elements_order = (pm.TechnicalRange,)


class StringMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = pm.StringMetricDescriptor
    STATE_QNAME = pm.StringMetricState


class EnumStringMetricDescriptorContainer(StringMetricDescriptorContainer):
    NODETYPE = pm.EnumStringMetricDescriptor
    STATE_QNAME = pm.EnumStringMetricState
    AllowedValue = cp.SubElementListProperty(pm.AllowedValue, value_class=pmtypes.AllowedValue)
    _props = ('AllowedValue',)
    _child_elements_order = (pm.AllowedValue,)


class RealTimeSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    is_realtime_sample_array_metric_descriptor = True
    NODETYPE = pm.RealTimeSampleArrayMetricDescriptor
    STATE_QNAME = pm.RealTimeSampleArrayMetricState
    TechnicalRange = cp.SubElementListProperty(pm.TechnicalRange, value_class=pmtypes.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    SamplePeriod = cp.DurationAttributeProperty('SamplePeriod', is_optional=False)
    _props = ('TechnicalRange', 'Resolution', 'SamplePeriod')
    _child_elements_order = (pm.TechnicalRange,)


class DistributionSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = pm.DistributionSampleArrayMetricDescriptor
    STATE_QNAME = pm.DistributionSampleArrayMetricState
    TechnicalRange = cp.SubElementListProperty(pm.TechnicalRange, value_class=pmtypes.Range)
    DomainUnit = cp.SubElementProperty(pm.DomainUnit, value_class=pmtypes.CodedValue)
    DistributionRange = cp.SubElementProperty(pm.DistributionRange, value_class=pmtypes.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    _props = ('TechnicalRange', 'DomainUnit', 'DistributionRange', 'Resolution')
    _child_elements_order = (pm.TechnicalRange,
                             pm.DomainUnit,
                             pm.DistributionRange)


class AbstractOperationDescriptorContainer(AbstractDescriptorContainer):
    is_operational_descriptor = True
    OperationTarget = cp.HandleRefAttributeProperty('OperationTarget', is_optional=False)
    MaxTimeToFinish = cp.DurationAttributeProperty('MaxTimeToFinish')  # optional  xsd:duration
    InvocationEffectiveTimeout = cp.DurationAttributeProperty('InvocationEffectiveTimeout')  # optional  xsd:duration
    Retriggerable = cp.BooleanAttributeProperty('Retriggerable', implied_py_value=True)  # optional
    AccessLevel = cp.EnumAttributeProperty('AccessLevel', implied_py_value=pmtypes.T_AccessLevel.USER,
                                           enum_cls=pmtypes.T_AccessLevel)
    _props = ('OperationTarget', 'MaxTimeToFinish', 'InvocationEffectiveTimeout', 'Retriggerable', 'AccessLevel')


class SetValueOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    NODETYPE = pm.SetValueOperationDescriptor
    STATE_QNAME = pm.SetValueOperationState


class SetStringOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    NODETYPE = pm.SetStringOperationDescriptor
    STATE_QNAME = pm.SetStringOperationState
    MaxLength = cp.IntegerAttributeProperty('MaxLength')
    _props = ('MaxLength',)


class AbstractSetStateOperationDescriptor(AbstractOperationDescriptorContainer):
    ModifiableData = cp.SubElementStringListProperty(pm.ModifiableData)
    _props = ('ModifiableData',)
    _child_elements_order = (pm.ModifiableData,)


class SetContextStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm.SetContextStateOperationDescriptor
    STATE_QNAME = pm.SetContextStateOperationState


class SetMetricStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm.SetMetricStateOperationDescriptor
    STATE_QNAME = pm.SetMetricStateOperationState


class SetComponentStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm.SetComponentStateOperationDescriptor
    STATE_QNAME = pm.SetComponentStateOperationState


class SetAlertStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm.SetAlertStateOperationDescriptor
    STATE_QNAME = pm.SetAlertStateOperationState


class ActivateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = pm.ActivateOperationDescriptor
    STATE_QNAME = pm.ActivateOperationState
    Argument = cp.SubElementListProperty(pm.Argument, value_class=pmtypes.ActivateOperationDescriptorArgument)
    _props = ('Argument',)
    _child_elements_order = (pm.Argument,)


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
    NODETYPE = pm.AlertSystemDescriptor
    STATE_QNAME = pm.AlertSystemState
    MaxPhysiologicalParallelAlarms = cp.UnsignedIntAttributeProperty('MaxPhysiologicalParallelAlarms')
    MaxTechnicalParallelAlarms = cp.UnsignedIntAttributeProperty('MaxTechnicalParallelAlarms')
    SelfCheckPeriod = cp.DurationAttributeProperty('SelfCheckPeriod')
    _props = ('MaxPhysiologicalParallelAlarms', 'MaxTechnicalParallelAlarms', 'SelfCheckPeriod')
    _child_elements_order = (pm.AlertCondition,
                             pm.AlertSignal)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.AlertCondition, (pm.AlertConditionDescriptor,
                                                   pm.LimitAlertConditionDescriptor
                                                   )
                               ),
        ChildDescriptorMapping(pm.AlertSignal, (pm.AlertSignalDescriptor,)),
    )


class AlertConditionDescriptorContainer(AbstractAlertDescriptorContainer):
    """An ALERT CONDITION contains the information about a potentially or actually HAZARDOUS SITUATION.
      Examples: a physiological alarm limit has been exceeded or a sensor has been unplugged."""
    is_alert_condition_descriptor = True
    NODETYPE = pm.AlertConditionDescriptor
    STATE_QNAME = pm.AlertConditionState
    Source = cp.SubElementHandleRefListProperty(pm.Source)  # a list of 0...n pm:HandleRef elements
    CauseInfo = cp.SubElementListProperty(pm.CauseInfo, value_class=pmtypes.CauseInfo)
    Kind = cp.EnumAttributeProperty('Kind', default_py_value=pmtypes.AlertConditionKind.OTHER,
                                    enum_cls=pmtypes.AlertConditionKind, is_optional=False)
    Priority = cp.EnumAttributeProperty('Priority', default_py_value=pmtypes.AlertConditionPriority.NONE,
                                        enum_cls=pmtypes.AlertConditionPriority, is_optional=False)
    DefaultConditionGenerationDelay = cp.DurationAttributeProperty('DefaultConditionGenerationDelay',
                                                                   implied_py_value=0)
    CanEscalate = cp.EnumAttributeProperty('CanEscalate', enum_cls=pmtypes.CanEscalateAlertConditionPriority)
    CanDeescalate = cp.EnumAttributeProperty('CanDeescalate', enum_cls=pmtypes.CanDeEscalateAlertConditionPriority)
    _props = ('Source', 'CauseInfo', 'Kind', 'Priority', 'DefaultConditionGenerationDelay',
              'CanEscalate', 'CanDeescalate')
    _child_elements_order = (pm.Source,
                             pm.CauseInfo)


class LimitAlertConditionDescriptorContainer(AlertConditionDescriptorContainer):
    NODETYPE = pm.LimitAlertConditionDescriptor
    STATE_QNAME = pm.LimitAlertConditionState
    MaxLimits = cp.SubElementProperty(pm.MaxLimits, value_class=pmtypes.Range, default_py_value=pmtypes.Range())
    AutoLimitSupported = cp.BooleanAttributeProperty('AutoLimitSupported', implied_py_value=False)
    _props = ('MaxLimits', 'AutoLimitSupported',)
    _child_elements_order = (pm.MaxLimits,)


class AlertSignalDescriptorContainer(AbstractAlertDescriptorContainer):
    is_alert_signal_descriptor = True
    NODETYPE = pm.AlertSignalDescriptor
    STATE_QNAME = pm.AlertSignalState
    ConditionSignaled = cp.HandleRefAttributeProperty('ConditionSignaled')
    Manifestation = cp.EnumAttributeProperty('Manifestation', enum_cls=pmtypes.AlertSignalManifestation,
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
    NODETYPE = pm.SystemContextDescriptor
    STATE_QNAME = pm.SystemContextState
    _child_elements_order = (pm.PatientContext,
                             pm.LocationContext,
                             pm.EnsembleContext,
                             pm.OperatorContext,
                             pm.WorkflowContext,
                             pm.MeansContext)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(pm.PatientContext, (pm.PatientContextDescriptor,)),
        ChildDescriptorMapping(pm.LocationContext, (pm.LocationContextDescriptor,)),
        ChildDescriptorMapping(pm.EnsembleContext, (pm.EnsembleContextDescriptor,)),
        ChildDescriptorMapping(pm.OperatorContext, (pm.OperatorContextDescriptor,)),
        ChildDescriptorMapping(pm.WorkflowContext, (pm.WorkflowContextDescriptor,)),
        ChildDescriptorMapping(pm.MeansContext, (pm.MeansContextDescriptor,)),
    )


class AbstractContextDescriptorContainer(AbstractDescriptorContainer):
    is_context_descriptor = True


class PatientContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm.PatientContextDescriptor
    STATE_QNAME = pm.PatientContextState


class LocationContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm.LocationContextDescriptor
    STATE_QNAME = pm.LocationContextState


class WorkflowContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm.WorkflowContextDescriptor
    STATE_QNAME = pm.WorkflowContextState


class OperatorContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm.OperatorContextDescriptor
    STATE_QNAME = pm.OperatorContextState


class MeansContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm.MeansContextDescriptor
    STATE_QNAME = pm.MeansContextState


class EnsembleContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = pm.EnsembleContextDescriptor
    STATE_QNAME = pm.EnsembleContextState


_classes = inspect.getmembers(sys.modules[__name__],
                              lambda member: inspect.isclass(member) and member.__module__ == __name__)
_classes_with_NODETYPE = [c[1] for c in _classes if hasattr(c[1], 'NODETYPE') and c[1].NODETYPE is not None]
# make a dictionary from found classes: (Key is NODETYPE, value is the class itself

_name_class_lookup = {c.NODETYPE: c for c in _classes_with_NODETYPE}

_name_class_xtra_lookup = {
    pm.Battery: BatteryDescriptorContainer,
    pm.Mds: MdsDescriptorContainer,
    pm.Vmd: VmdDescriptorContainer,
    pm.Sco: ScoDescriptorContainer,
    pm.Channel: ChannelDescriptorContainer,
    pm.Clock: ClockDescriptorContainer,
    pm.SystemContext: SystemContextDescriptorContainer,
    pm.PatientContext: PatientContextDescriptorContainer,
    pm.LocationContext: LocationContextDescriptorContainer,
    pm.WorkflowContext: WorkflowContextDescriptorContainer,
    pm.OperatorContext: OperatorContextDescriptorContainer,
    pm.MeansContext: MeansContextDescriptorContainer,
    pm.EnsembleContext: EnsembleContextDescriptorContainer,
    pm.AlertSystem: AlertSystemDescriptorContainer,
    pm.AlertCondition: AlertConditionDescriptorContainer,
    pm.AlertSignal: AlertSignalDescriptorContainer,
}
_name_class_lookup.update(_name_class_xtra_lookup)


def get_container_class(qname):
    """
    :param qname: a QName instance
    """
    return _name_class_lookup.get(qname)
