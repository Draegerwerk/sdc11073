import inspect
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Tuple

from lxml import etree as etree_

from . import containerproperties as cp
from .containerbase import ContainerBase
from .. import msgtypes
from .. import observableproperties as properties
from .. import pmtypes
from ..namespaces import domTag, extTag, msgTag, Prefixes


@dataclass(frozen=True)
class ChildDescriptorMapping:
    """Maps element names to node types. Needed when building an xml tree of descriptors.
    The name of a child element is often not identical to the type of a descriptor, e.g. a channel uses
    domTag('Metric') for all classes derived from AbstractMetricDescriptor. """
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


def make_descriptor_node(descriptor_container, tag, nsmapper, set_xsi_type=True, connect_child_descriptors=False):
    """
    Creates an lxml etree node from instance data.
    :param descriptor_container: a descriptor container instance
    :param nsmapper:  namespaces.DocNamespaceHelper instance
    :param set_xsi_type: if true, the NODETYPE will be used to set the xsi:type attribute of the node
    :param tag: tag of node
    :param connect_child_descriptors: if True, the whole sub-tree is included
    :return: an etree node
    """
    if set_xsi_type:
        namespace_map = nsmapper.partial_map(Prefixes.PM, Prefixes.XSI)
    else:
        namespace_map = nsmapper.partial_map(Prefixes.PM)
    node = etree_.Element(tag,
                          attrib={'Handle': descriptor_container.Handle},
                          nsmap=namespace_map)
    descriptor_container.update_node(node, nsmapper, set_xsi_type)  # create all
    if connect_child_descriptors:
        # append all child containers, then bring all child elements in correct order
        for node_type, child_list in descriptor_container.child_containers_by_type.items():
            child_tag, set_xsi = descriptor_container.tag_name_for_child_descriptor(node_type)
            for child in child_list:
                child_node = make_descriptor_node(child, child_tag, nsmapper, set_xsi, connect_child_descriptors=True)
                node.append(child_node)
    descriptor_container.sort_child_nodes(node)
    return node


class AbstractDescriptorContainer(ContainerBase):
    """
    This class represents the AbstractDescriptor
    """
    # these class variables allow easy type-checking. Derived classes will set corresponding values to True
    # pylint: disable=invalid-name
    is_system_context_descriptor = False
    is_realtime_sample_array_metric_descriptor = False
    is_metric_descriptor = False
    is_operational_descriptor = False
    is_component_descriptor = False
    is_alert_descriptor = False
    is_alert_signal_descriptor = False
    is_alert_condition_descriptor = False
    is_context_descriptor = False

    is_leaf = True # determines if children can be added

    node = properties.ObservableProperty()  # the etree node

    Handle = cp.HandleAttributeProperty('Handle', is_optional=False)
    Extension = cp.ExtensionNodeProperty()
    DescriptorVersion = cp.VersionCounterAttributeProperty('DescriptorVersion',
                                                           default_py_value=0)
    SafetyClassification = cp.EnumAttributeProperty('SafetyClassification',
                                                    implied_py_value=pmtypes.SafetyClassification.INF,
                                                    enum_cls=pmtypes.SafetyClassification)
    Type = cp.SubElementProperty(domTag('Type'), value_class=pmtypes.CodedValue)
    # pylint: enable=invalid-name
    _props = ('Handle', 'DescriptorVersion', 'SafetyClassification', 'Extension', 'Type')
    _child_elements_order = (extTag('Extension'), domTag('Type'))  # child elements in in BICEPS order
    STATE_QNAME = None
    extension_class_lookup = {msgTag('Retrievability'): msgtypes.Retrievability}

    def __init__(self, handle, parent_handle):
        super().__init__()
        self.parent_handle = parent_handle
        self.Handle = handle
        self.child_containers_by_type = defaultdict(list)

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
    def retrievability(self) -> [msgtypes.Retrievability, None]:
        if self.Extension is None:
            return None
        return self.Extension.value.get(msgTag('Retrievability'))

    @retrievability.setter
    def retrievability(self, retrievability_instance: msgtypes.Retrievability):
        value = self.Extension.value
        value[msgTag('Retrievability')] = retrievability_instance

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
                f'Update from a container with different handle is not possible! ' \
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
        :param connect_child_descriptors: if True, the whole sub-tree is included
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

    def __str__(self):
        name = self.NODETYPE.localname or None
        return f'Descriptor "{name}": handle={self.Handle} descriptor version={self.DescriptorVersion} ' \
               f'parent handle={self.parent_handle}'

    def __repr__(self):
        name = self.NODETYPE.localname or None
        return f'Descriptor "{name}": handle={self.Handle} descriptor version={self.DescriptorVersion} ' \
               f'parent={self.parent_handle}'

    @classmethod
    def from_node(cls, node, parent_handle):
        obj = cls(handle=None,  # will be determined in constructor from node value
                  parent_handle=parent_handle)
        obj.update_from_node(node)
        return obj


class AbstractDeviceComponentDescriptorContainer(AbstractDescriptorContainer):
    is_component_descriptor = True
    is_leaf = False
    ProductionSpecification = cp.SubElementListProperty(domTag('ProductionSpecification'),
                                                        value_class=pmtypes.ProductionSpecification)
    _props = ('ProductionSpecification',)
    _child_elements_order = (domTag('ProductionSpecification'),)


class AbstractComplexDeviceComponentDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    _child_elements_order = (domTag('AlertSystem'), domTag('Sco'))
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('AlertSystem'), (domTag('AlertSystemDescriptor'),)),
        ChildDescriptorMapping(domTag('Sco'), (domTag('ScoDescriptor'),)))


class MdsDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    NODETYPE = domTag('MdsDescriptor')
    STATE_QNAME = domTag('MdsState')
    # pylint: disable=invalid-name
    MetaData = cp.SubElementProperty(domTag('MetaData'), value_class=pmtypes.MetaData, is_optional=True)
    ApprovedJurisdictions = cp.SubElementProperty(domTag('ApprovedJurisdictions'),
                                                  value_class=pmtypes.ApprovedJurisdictions,
                                                  is_optional=True)
    # pylint: enable=invalid-name
    _props = ('MetaData', 'ApprovedJurisdictions')
    _child_elements_order = (domTag('MetaData'),
                             domTag('SystemContext'),
                             domTag('Clock'),
                             domTag('Battery'),
                             domTag('ApprovedJurisdictions'),
                             domTag('Vmd'))
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('SystemContext'), (domTag('SystemContextDescriptor'),)),
        ChildDescriptorMapping(domTag('Clock'), (domTag('ClockDescriptor'),)),
        ChildDescriptorMapping(domTag('Battery'), (domTag('BatteryDescriptor'),)),
        ChildDescriptorMapping(domTag('Vmd'), (domTag('VmdDescriptor'),)),
    )


class VmdDescriptorContainer(AbstractComplexDeviceComponentDescriptorContainer):
    NODETYPE = domTag('VmdDescriptor')
    STATE_QNAME = domTag('VmdState')

    ApprovedJurisdictions = cp.SubElementProperty(domTag('ApprovedJurisdictions'),
                                                  value_class=pmtypes.ApprovedJurisdictions,
                                                  is_optional=True)
    _props = ('ApprovedJurisdictions',)
    _child_elements_order = (domTag('ApprovedJurisdictions'),
                             domTag('Channel'),)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('Channel'), (domTag('ChannelDescriptor'),)),
    )


class ChannelDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = domTag('ChannelDescriptor')
    STATE_QNAME = domTag('ChannelState')
    _child_elements_order = (domTag('Metric'),)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('Metric'), (domTag('NumericMetricDescriptor'),
                                                  domTag('StringMetricDescriptor'),
                                                  domTag('EnumStringMetricDescriptor'),
                                                  domTag('RealTimeSampleArrayMetricDescriptor'),
                                                  domTag('DistributionSampleArrayMetricDescriptor'),
                                                  )
                               ),
    )


class ClockDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = domTag('ClockDescriptor')
    STATE_QNAME = domTag('ClockState')
    # pylint: disable=invalid-name
    TimeProtocol = cp.SubElementListProperty(domTag('TimeProtocol'), value_class=pmtypes.CodedValue)
    Resolution = cp.DurationAttributeProperty('Resolution')  # optional,  xsd:duration
    # pylint: enable=invalid-name
    _props = ('TimeProtocol', 'Resolution')
    _child_elements_order = (domTag('TimeProtocol'),)


class BatteryDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = domTag('BatteryDescriptor')
    STATE_QNAME = domTag('BatteryState')
    # pylint: disable=invalid-name
    CapacityFullCharge = cp.SubElementProperty(domTag('CapacityFullCharge'),
                                               value_class=pmtypes.Measurement)  # optional
    CapacitySpecified = cp.SubElementProperty(domTag('CapacitySpecified'), value_class=pmtypes.Measurement)  # optional
    VoltageSpecified = cp.SubElementProperty(domTag('VoltageSpecified'), value_class=pmtypes.Measurement)  # optional
    # pylint: enable=invalid-name
    _props = ('CapacityFullCharge', 'CapacitySpecified', 'VoltageSpecified')
    _child_elements_order = (domTag('CapacityFullCharge'),
                             domTag('CapacitySpecified'),
                             domTag('VoltageSpecified'))


class ScoDescriptorContainer(AbstractDeviceComponentDescriptorContainer):
    NODETYPE = domTag('ScoDescriptor')
    STATE_QNAME = domTag('ScoState')
    _child_elements_order = (domTag('Operation'),)
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('Operation'), (domTag('SetValueOperationDescriptor'),
                                                     domTag('SetStringOperationDescriptor'),
                                                     domTag('SetContextStateOperationDescriptor'),
                                                     domTag('SetMetricStateOperationDescriptor'),
                                                     domTag('SetComponentStateOperationDescriptor'),
                                                     domTag('SetAlertStateOperationDescriptor'),
                                                     domTag('ActivateOperationDescriptor')
                                                     )
                               ),
    )


class AbstractMetricDescriptorContainer(AbstractDescriptorContainer):
    is_metric_descriptor = True
    Unit = cp.SubElementProperty(domTag('Unit'), value_class=pmtypes.CodedValue)
    BodySite = cp.SubElementListProperty(domTag('BodySite'), value_class=pmtypes.CodedValue)
    Relation = cp.SubElementListProperty(domTag('Relation'), value_class=pmtypes.Relation)  # o...n
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
    _child_elements_order = (domTag('Unit'),
                             domTag('BodySite'),
                             domTag('Relation'))


class NumericMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = domTag('NumericMetricDescriptor')
    STATE_QNAME = domTag('NumericMetricState')
    TechnicalRange = cp.SubElementListProperty(domTag('TechnicalRange'), value_class=pmtypes.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    AveragingPeriod = cp.DurationAttributeProperty('AveragingPeriod')  # optional
    _props = ('TechnicalRange', 'Resolution', 'AveragingPeriod')
    _child_elements_order = (domTag('TechnicalRange'),)


class StringMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = domTag('StringMetricDescriptor')
    STATE_QNAME = domTag('StringMetricState')


class EnumStringMetricDescriptorContainer(StringMetricDescriptorContainer):
    NODETYPE = domTag('EnumStringMetricDescriptor')
    STATE_QNAME = domTag('EnumStringMetricState')
    AllowedValue = cp.SubElementListProperty(domTag('AllowedValue'), value_class=pmtypes.AllowedValue)
    _props = ('AllowedValue',)
    _child_elements_order = (domTag('AllowedValue'),)


class RealTimeSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    is_realtime_sample_array_metric_descriptor = True
    NODETYPE = domTag('RealTimeSampleArrayMetricDescriptor')
    STATE_QNAME = domTag('RealTimeSampleArrayMetricState')
    TechnicalRange = cp.SubElementListProperty(domTag('TechnicalRange'), value_class=pmtypes.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    SamplePeriod = cp.DurationAttributeProperty('SamplePeriod', is_optional=False)
    _props = ('TechnicalRange', 'Resolution', 'SamplePeriod')
    _child_elements_order = (domTag('TechnicalRange'),)


class DistributionSampleArrayMetricDescriptorContainer(AbstractMetricDescriptorContainer):
    NODETYPE = domTag('DistributionSampleArrayMetricDescriptor')
    STATE_QNAME = domTag('DistributionSampleArrayMetricState')
    TechnicalRange = cp.SubElementListProperty(domTag('TechnicalRange'), value_class=pmtypes.Range)
    DomainUnit = cp.SubElementProperty(domTag('DomainUnit'), value_class=pmtypes.CodedValue)
    DistributionRange = cp.SubElementProperty(domTag('DistributionRange'), value_class=pmtypes.Range)
    Resolution = cp.DecimalAttributeProperty('Resolution', is_optional=False)
    _props = ('TechnicalRange', 'DomainUnit', 'DistributionRange', 'Resolution')
    _child_elements_order = (domTag('TechnicalRange'),
                             domTag('DomainUnit'),
                             domTag('DistributionRange'))


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
    NODETYPE = domTag('SetValueOperationDescriptor')
    STATE_QNAME = domTag('SetValueOperationState')


class SetStringOperationDescriptorContainer(AbstractOperationDescriptorContainer):
    NODETYPE = domTag('SetStringOperationDescriptor')
    STATE_QNAME = domTag('SetStringOperationState')
    MaxLength = cp.IntegerAttributeProperty('MaxLength')
    _props = ('MaxLength',)


class AbstractSetStateOperationDescriptor(AbstractOperationDescriptorContainer):
    ModifiableData = cp.SubElementTextListProperty(domTag('ModifiableData'))
    _props = ('ModifiableData',)
    _child_elements_order = (domTag('ModifiableData'),)


class SetContextStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = domTag('SetContextStateOperationDescriptor')
    STATE_QNAME =domTag('SetContextStateOperationState')


class SetMetricStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = domTag('SetMetricStateOperationDescriptor')
    STATE_QNAME = domTag('SetMetricStateOperationState')


class SetComponentStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = domTag('SetComponentStateOperationDescriptor')
    STATE_QNAME = domTag('SetComponentStateOperationState')


class SetAlertStateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = domTag('SetAlertStateOperationDescriptor')
    STATE_QNAME =domTag('SetAlertStateOperationState')


class ActivateOperationDescriptorContainer(AbstractSetStateOperationDescriptor):
    NODETYPE = domTag('ActivateOperationDescriptor')
    STATE_QNAME = domTag('ActivateOperationState')
    Argument = cp.SubElementListProperty(domTag('Argument'), value_class=pmtypes.ActivateOperationDescriptorArgument)
    _props = ('Argument',)
    _child_elements_order = (domTag('Argument'),)


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
    NODETYPE = domTag('AlertSystemDescriptor')
    STATE_QNAME = domTag('AlertSystemState')
    MaxPhysiologicalParallelAlarms = cp.UnsignedIntAttributeProperty('MaxPhysiologicalParallelAlarms')
    MaxTechnicalParallelAlarms = cp.UnsignedIntAttributeProperty('MaxTechnicalParallelAlarms')
    SelfCheckPeriod = cp.DurationAttributeProperty('SelfCheckPeriod')
    _props = ('MaxPhysiologicalParallelAlarms', 'MaxTechnicalParallelAlarms', 'SelfCheckPeriod')
    _child_elements_order = (domTag('AlertCondition'),
                             domTag('AlertSignal'))
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('AlertCondition'), (domTag('AlertConditionDescriptor'),
                                                          domTag('LimitAlertConditionDescriptor')
                                                          )
                               ),
        ChildDescriptorMapping(domTag('AlertSignal'), (domTag('AlertSignalDescriptor'),)),
    )


class AlertConditionDescriptorContainer(AbstractAlertDescriptorContainer):
    """An ALERT CONDITION contains the information about a potentially or actually HAZARDOUS SITUATION.
      Examples: a physiological alarm limit has been exceeded or a sensor has been unplugged."""
    is_alert_condition_descriptor = True
    NODETYPE = domTag('AlertConditionDescriptor')
    STATE_QNAME = domTag('AlertConditionState')
    Source = cp.SubElementHandleRefListProperty(domTag('Source'))  # a list of 0...n pm:HandleRef elements
    CauseInfo = cp.SubElementListProperty(domTag('CauseInfo'), value_class=pmtypes.CauseInfo)
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
    _child_elements_order = (domTag('Source'),
                             domTag('CauseInfo'))


class LimitAlertConditionDescriptorContainer(AlertConditionDescriptorContainer):
    NODETYPE = domTag('LimitAlertConditionDescriptor')
    STATE_QNAME =domTag('LimitAlertConditionState')
    MaxLimits = cp.SubElementProperty(domTag('MaxLimits'), value_class=pmtypes.Range, default_py_value=pmtypes.Range())
    AutoLimitSupported = cp.BooleanAttributeProperty('AutoLimitSupported', implied_py_value=False)
    _props = ('MaxLimits', 'AutoLimitSupported',)
    _child_elements_order = (domTag('MaxLimits'),)


class AlertSignalDescriptorContainer(AbstractAlertDescriptorContainer):
    is_alert_signal_descriptor = True
    NODETYPE = domTag('AlertSignalDescriptor')
    STATE_QNAME = domTag('AlertSignalState')
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
    NODETYPE = domTag('SystemContextDescriptor')
    STATE_QNAME = domTag('SystemContextState')
    _child_elements_order = (domTag('PatientContext'),
                             domTag('LocationContext'),
                             domTag('EnsembleContext'),
                             domTag('OperatorContext'),
                             domTag('WorkflowContext'),
                             domTag('MeansContext'))
    _child_descriptor_name_mappings = (
        ChildDescriptorMapping(domTag('PatientContext'), (domTag('PatientContextDescriptor'),)),
        ChildDescriptorMapping(domTag('LocationContext'), (domTag('LocationContextDescriptor'),)),
        ChildDescriptorMapping(domTag('EnsembleContext'), (domTag('EnsembleContextDescriptor'),)),
        ChildDescriptorMapping(domTag('OperatorContext'), (domTag('OperatorContextDescriptor'),)),
        ChildDescriptorMapping(domTag('WorkflowContext'), (domTag('WorkflowContextDescriptor'),)),
        ChildDescriptorMapping(domTag('MeansContext'), (domTag('MeansContextDescriptor'),)),
    )


class AbstractContextDescriptorContainer(AbstractDescriptorContainer):
    is_context_descriptor = True


class PatientContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = domTag('PatientContextDescriptor')
    STATE_QNAME = domTag('PatientContextState')


class LocationContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = domTag('LocationContextDescriptor')
    STATE_QNAME = domTag('LocationContextState')


class WorkflowContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = domTag('WorkflowContextDescriptor')
    STATE_QNAME = domTag('WorkflowContextState')


class OperatorContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = domTag('OperatorContextDescriptor')
    STATE_QNAME = domTag('OperatorContextState')


class MeansContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = domTag('MeansContextDescriptor')
    STATE_QNAME = domTag('MeansContextState')


class EnsembleContextDescriptorContainer(AbstractContextDescriptorContainer):
    NODETYPE = domTag('EnsembleContextDescriptor')
    STATE_QNAME = domTag('EnsembleContextState')


_name_class_lookup = {
    domTag('Battery'): BatteryDescriptorContainer,
    domTag('BatteryDescriptor'): BatteryDescriptorContainer,
    domTag('Mds'): MdsDescriptorContainer,
    domTag('MdsDescriptor'): MdsDescriptorContainer,
    domTag('Vmd'): VmdDescriptorContainer,
    domTag('VmdDescriptor'): VmdDescriptorContainer,
    domTag('Sco'): ScoDescriptorContainer,
    domTag('ScoDescriptor'): ScoDescriptorContainer,
    domTag('Channel'): ChannelDescriptorContainer,
    domTag('ChannelDescriptor'): ChannelDescriptorContainer,
    domTag('Clock'): ClockDescriptorContainer,
    domTag('ClockDescriptor'): ClockDescriptorContainer,
    domTag('SystemContext'): SystemContextDescriptorContainer,
    domTag('SystemContextDescriptor'): SystemContextDescriptorContainer,
    domTag('PatientContext'): PatientContextDescriptorContainer,
    domTag('LocationContext'): LocationContextDescriptorContainer,
    domTag('PatientContextDescriptor'): PatientContextDescriptorContainer,
    domTag('LocationContextDescriptor'): LocationContextDescriptorContainer,
    domTag('WorkflowContext'): WorkflowContextDescriptorContainer,
    domTag('WorkflowContextDescriptor'): WorkflowContextDescriptorContainer,
    domTag('OperatorContext'): OperatorContextDescriptorContainer,
    domTag('OperatorContextDescriptor'): OperatorContextDescriptorContainer,
    domTag('MeansContext'): MeansContextDescriptorContainer,
    domTag('MeansContextDescriptor'): MeansContextDescriptorContainer,
    domTag('EnsembleContext'): EnsembleContextDescriptorContainer,
    domTag('EnsembleContextDescriptor'): EnsembleContextDescriptorContainer,
    domTag('AlertSystem'): AlertSystemDescriptorContainer,
    domTag('AlertSystemDescriptor'): AlertSystemDescriptorContainer,
    domTag('AlertCondition'): AlertConditionDescriptorContainer,
    domTag('AlertConditionDescriptor'): AlertConditionDescriptorContainer,
    domTag('AlertSignal'): AlertSignalDescriptorContainer,
    domTag('AlertSignalDescriptor'): AlertSignalDescriptorContainer,
    domTag('StringMetricDescriptor'): StringMetricDescriptorContainer,
    domTag('EnumStringMetricDescriptor'): EnumStringMetricDescriptorContainer,
    domTag('NumericMetricDescriptor'): NumericMetricDescriptorContainer,
    domTag('RealTimeSampleArrayMetricDescriptor'): RealTimeSampleArrayMetricDescriptorContainer,
    domTag('DistributionSampleArrayMetricDescriptor'): DistributionSampleArrayMetricDescriptorContainer,
    domTag('LimitAlertConditionDescriptor'): LimitAlertConditionDescriptorContainer,
    domTag('SetValueOperationDescriptor'): SetValueOperationDescriptorContainer,
    domTag('SetStringOperationDescriptor'): SetStringOperationDescriptorContainer,
    domTag('ActivateOperationDescriptor'): ActivateOperationDescriptorContainer,
    domTag('SetContextStateOperationDescriptor'): SetContextStateOperationDescriptorContainer,
    domTag('SetMetricStateOperationDescriptor'): SetMetricStateOperationDescriptorContainer,
    domTag('SetComponentStateOperationDescriptor'): SetComponentStateOperationDescriptorContainer,
    domTag('SetAlertStateOperationDescriptor'): SetAlertStateOperationDescriptorContainer,
}


def get_container_class(qname):
    """
    :param qname: a QName instance
    """
    return _name_class_lookup.get(qname)
