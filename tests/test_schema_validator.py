"""Test for the schema validator."""

import lxml.etree

from sdc11073 import namespaces, schema_resolver


def test_schema_validator_does_not_raise_schema_validation_error_with_valid_input():
    """Cover issue 432."""
    get_mdib_response = lxml.etree.fromstring("""<msg:GetMdibResponse xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension" xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant" xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message" xmlns:sdpi="urn:oid:1.3.6.1.4.1.19376.1.6.2.10.1.1.1" xmlns:mpkp="urn:oid:1.3.111.2.11073.10701.3.1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" SequenceId="urn:uuid:90521c43-4822-4f01-b2e2-7fd0d5f4a31e">
    <msg:Mdib SequenceId="urn:uuid:90521c43-4822-4f01-b2e2-7fd0d5f4a31e">
        <pm:MdDescription>
            <pm:Mds Handle="mds_0">
                <ext:Extension>
                    <sdpi:CodedAttributes>
                        <sdpi:CodedStringAttribute>
                            <sdpi:MdcAttribute Code="67886" CodingSystem="urn:oid:1.3.111.2.11073.10101.1"/>
                            <sdpi:Value>equipment-label</sdpi:Value>
                        </sdpi:CodedStringAttribute>
                    </sdpi:CodedAttributes>
                </ext:Extension>
                <pm:Type Code="67108865">
                    <pm:ConceptDescription Lang="en-US">SDPi Test MDS</pm:ConceptDescription>
                </pm:Type>
                <pm:AlertSystem SelfCheckPeriod="PT5S" Handle="alert_system.mds_0">
                    <pm:AlertCondition xsi:type="pm:LimitAlertConditionDescriptor" AutoLimitSupported="false" Kind="Phy" Priority="Me" Handle="alert_condition_0.mds_0">
                        <pm:Type Code="67108884"/>
                        <pm:Source>numeric_metric_1.channel_1.vmd_0.mds_0</pm:Source>
                        <pm:MaxLimits Lower="10" Upper="90"/>
                    </pm:AlertCondition>
                    <pm:AlertSignal ConditionSignaled="alert_condition_0.mds_0" Manifestation="Vis" Latching="false" AcknowledgementSupported="false" Handle="alert_signal_0.mds_0"/>
                </pm:AlertSystem>
                <pm:Sco Handle="sco.mds_0">
                    <pm:Operation xsi:type="pm:SetContextStateOperationDescriptor" OperationTarget="patient_context.mds_0" MaxTimeToFinish="PT1S" Retriggerable="false" Handle="set_context_0.sco.mds_0" SafetyClassification="MedA">
                        <pm:Type Code="67108887">
                            <pm:ConceptDescription Lang="en-US">Adds a exactly one Patient Context to the operation target by using the attributes and elements from the SCO Operation's payload</pm:ConceptDescription>
                        </pm:Type>
                    </pm:Operation>
                    <pm:Operation xsi:type="pm:SetValueOperationDescriptor" OperationTarget="numeric_metric_0.channel_0.vmd_0.mds_0" Retriggerable="false" Handle="set_value_0.sco.mds_0" SafetyClassification="MedA">
                        <pm:Type Code="67108888">
                            <pm:ConceptDescription Lang="en-US">Sets the value of the targeted Numeric Metric</pm:ConceptDescription>
                        </pm:Type>
                    </pm:Operation>
                    <pm:Operation xsi:type="pm:SetStringOperationDescriptor" OperationTarget="enum_string_metric_0.channel_0.vmd_0.mds_0" Retriggerable="false" Handle="set_string_0.sco.mds_0" SafetyClassification="MedA">
                        <pm:Type Code="67108889">
                            <pm:ConceptDescription Lang="en-US">Sets the value of the targeted Enum String Metric</pm:ConceptDescription>
                        </pm:Type>
                    </pm:Operation>
                    <pm:Operation xsi:type="pm:ActivateOperationDescriptor" OperationTarget="mds_0" Retriggerable="false" Handle="activate_0.sco.mds_0">
                        <pm:Type Code="67108893">
                            <pm:ConceptDescription Lang="en-US">Performs nothing. The operational state will be toggled periodically at least every 5 seconds in order to produce Operational State Reports.</pm:ConceptDescription>
                        </pm:Type>
                    </pm:Operation>
                    <pm:Operation xsi:type="pm:ActivateOperationDescriptor" OperationTarget="string_metric_1.channel_0.vmd_0.mds_0" Retriggerable="false" Handle="activate_1.sco.mds_0" SafetyClassification="MedA">
                        <pm:Type Code="67108893">
                            <pm:ConceptDescription Lang="en-US">Accepts three arguments which are concatenated and written to the metric value of the operation target.</pm:ConceptDescription>
                        </pm:Type>
                        <pm:Argument>
                            <pm:ArgName Code="67108895"/>
                            <pm:Arg xmlns="http://www.w3.org/2001/XMLSchema">string</pm:Arg>
                        </pm:Argument>
                        <pm:Argument>
                            <pm:ArgName Code="67108896"/>
                            <pm:Arg xmlns="http://www.w3.org/2001/XMLSchema">decimal</pm:Arg>
                        </pm:Argument>
                        <pm:Argument>
                            <pm:ArgName Code="67108897"/>
                            <pm:Arg xmlns="http://www.w3.org/2001/XMLSchema">anyURI</pm:Arg>
                        </pm:Argument>
                    </pm:Operation>
                </pm:Sco>
                <pm:SystemContext Handle="system_context.mds_0">
                    <pm:PatientContext Handle="patient_context.mds_0"/>
                    <pm:LocationContext Handle="location_context.mds_0"/>
                </pm:SystemContext>
                <pm:Clock Handle="clock.mds_0">
                    <pm:TimeProtocol Code="532224">
                        <pm:ConceptDescription Lang="en-US">None</pm:ConceptDescription>
                    </pm:TimeProtocol>
                    <pm:TimeProtocol Code="532225">
                        <pm:ConceptDescription Lang="en-US">NTPv4</pm:ConceptDescription>
                    </pm:TimeProtocol>
                    <pm:TimeProtocol Code="532234">
                        <pm:ConceptDescription Lang="en-US">EBWW</pm:ConceptDescription>
                    </pm:TimeProtocol>
                </pm:Clock>
                <pm:Battery Handle="battery_0.mds_0">
                    <pm:CapacityFullCharge MeasuredValue="100">
                        <pm:MeasurementUnit Code="268224">
                            <pm:ConceptDescription Lang="en-US">Magnitude ampere(s) hour</pm:ConceptDescription>
                        </pm:MeasurementUnit>
                    </pm:CapacityFullCharge>
                    <pm:VoltageSpecified MeasuredValue="230">
                        <pm:MeasurementUnit Code="266400">
                            <pm:ConceptDescription Lang="en-US">Magnitude volt(s)</pm:ConceptDescription>
                        </pm:MeasurementUnit>
                    </pm:VoltageSpecified>
                </pm:Battery>
                <pm:Vmd Handle="vmd_0.mds_0">
                    <pm:Type Code="67108868">
                        <pm:ConceptDescription Lang="en-US">SDPi Test VMD that contains settings and measurements including waveforms</pm:ConceptDescription>
                    </pm:Type>
                    <pm:Channel Handle="channel_0.vmd_0.mds_0">
                        <pm:Type Code="67108871">
                            <pm:ConceptDescription Lang="en-US">Channel that contains settings</pm:ConceptDescription>
                        </pm:Type>
                        <pm:Metric xsi:type="pm:NumericMetricDescriptor" Resolution="0.1" MetricCategory="Set" MetricAvailability="Intr" Handle="numeric_metric_0.channel_0.vmd_0.mds_0">
                            <pm:Type Code="67108874">
<pm:ConceptDescription Lang="en-US">Numeric setting, externally controllable</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                            <pm:TechnicalRange Lower="1" Upper="100" StepWidth="1"/>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:EnumStringMetricDescriptor" MetricCategory="Set" MetricAvailability="Intr" Handle="enum_string_metric_0.channel_0.vmd_0.mds_0">
                            <pm:Type Code="67108875">
<pm:ConceptDescription Lang="en-US">Enum setting, externally controllable</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                            <pm:AllowedValue>
<pm:Value>ON</pm:Value>
<pm:Type Code="67108877">
    <pm:ConceptDescription Lang="en-US">Enum Value ON</pm:ConceptDescription>
</pm:Type>
                            </pm:AllowedValue>
                            <pm:AllowedValue>
<pm:Value>OFF</pm:Value>
<pm:Type Code="67108878">
    <pm:ConceptDescription Lang="en-US">Enum Value OFF</pm:ConceptDescription>
</pm:Type>
                            </pm:AllowedValue>
                            <pm:AllowedValue>
<pm:Value>STANDBY</pm:Value>
<pm:Type Code="67108879">
    <pm:ConceptDescription Lang="en-US">Enum Value STANDBY</pm:ConceptDescription>
</pm:Type>
                            </pm:AllowedValue>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:StringMetricDescriptor" MetricCategory="Set" MetricAvailability="Intr" Handle="string_metric_0.channel_0.vmd_0.mds_0">
                            <pm:Type Code="67108876">
<pm:ConceptDescription Lang="en-US">String setting</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:StringMetricDescriptor" MetricCategory="Set" MetricAvailability="Intr" Handle="string_metric_1.channel_0.vmd_0.mds_0">
                            <pm:Type Code="67108898">
<pm:ConceptDescription Lang="en-US">Operation target of the activate operation</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                    </pm:Channel>
                    <pm:Channel Handle="channel_1.vmd_0.mds_0">
                        <pm:Type Code="67108872">
                            <pm:ConceptDescription Lang="en-US">Channel that contains measurements</pm:ConceptDescription>
                        </pm:Type>
                        <pm:Metric xsi:type="pm:NumericMetricDescriptor" Resolution="0.1" MetricCategory="Msrmt" MetricAvailability="Intr" DeterminationPeriod="PT5S" Handle="numeric_metric_1.channel_1.vmd_0.mds_0">
                            <pm:Type Code="67108880">
<pm:ConceptDescription Lang="en-US">Periodically determined intermittent numeric measurement metric</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                            <pm:TechnicalRange Lower="1" Upper="100" StepWidth="1"/>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:RealTimeSampleArrayMetricDescriptor" Resolution="1" SamplePeriod="PT0.01S" MetricCategory="Msrmt" MetricAvailability="Cont" Handle="rtsa_metric_0.channel_1.vmd_0.mds_0">
                            <pm:Type Code="67108881">
<pm:ConceptDescription Lang="en-US">Waveform metric 1</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:RealTimeSampleArrayMetricDescriptor" Resolution="1" SamplePeriod="PT0.01S" MetricCategory="Msrmt" MetricAvailability="Cont" Handle="rtsa_metric_1.channel_1.vmd_0.mds_0">
                            <pm:Type Code="67108882">
<pm:ConceptDescription Lang="en-US">Waveform metric 2</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:RealTimeSampleArrayMetricDescriptor" Resolution="1" SamplePeriod="PT0.01S" MetricCategory="Msrmt" MetricAvailability="Cont" Handle="rtsa_metric_2.channel_1.vmd_0.mds_0">
                            <pm:Type Code="67108899">
<pm:ConceptDescription Lang="en-US">Waveform metric 3</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:RealTimeSampleArrayMetricDescriptor" Resolution="1" SamplePeriod="PT0.5S" MetricCategory="Msrmt" MetricAvailability="Cont" Handle="rtsa_metric_3.channel_1.vmd_0.mds_0">
                            <pm:Type Code="67108883">
<pm:ConceptDescription Lang="en-US">Waveform metric 4</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                    </pm:Channel>
                </pm:Vmd>
                <pm:Vmd Handle="vmd_1.mds_0">
                    <pm:Type Code="67108869">
                        <pm:ConceptDescription Lang="en-US">SDPi Test VMD that contains settings to be externally controlled by bulk operations</pm:ConceptDescription>
                    </pm:Type>
                    <pm:Sco Handle="sco.vmd1.mds_0">
                        <pm:Operation xsi:type="pm:SetMetricStateOperationDescriptor" OperationTarget="channel_0.vmd_1.mds_0" Retriggerable="false" Handle="set_metric_0.sco.vmd_1.mds_0" SafetyClassification="MedA">
                            <pm:Type Code="67108890">
<pm:ConceptDescription Lang="en-US">Sets the @Value of 2 metric states at once</pm:ConceptDescription>
                            </pm:Type>
                        </pm:Operation>
                    </pm:Sco>
                    <pm:Channel Handle="channel_0.vmd_1.mds_0">
                        <pm:Type Code="67108871">
                            <pm:ConceptDescription Lang="en-US">Channel that contains settings to be externally controlled by bulk operations</pm:ConceptDescription>
                        </pm:Type>
                        <pm:Metric xsi:type="pm:NumericMetricDescriptor" Resolution="1" MetricCategory="Set" MetricAvailability="Intr" Handle="numeric_metric_0.channel_0.vmd_1.mds_0">
                            <pm:Type Code="67108891">
<pm:ConceptDescription Lang="en-US">Numeric setting, externally controllable by bulk update</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                        <pm:Metric xsi:type="pm:NumericMetricDescriptor" Resolution="1" MetricCategory="Set" MetricAvailability="Intr" Handle="numeric_metric_1.channel_0.vmd_1.mds_0">
                            <pm:Type Code="67108892">
<pm:ConceptDescription Lang="en-US">Numeric setting, externally controllable by bulk update</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="262656">
<pm:ConceptDescription Lang="en-US">Dimensionless</pm:ConceptDescription>
                            </pm:Unit>
                        </pm:Metric>
                    </pm:Channel>
                </pm:Vmd>
            </pm:Mds>
            <pm:Mds Handle="mds_1">
                <pm:Type Code="67108866">
                    <pm:ConceptDescription Lang="en-US">SDPi Test MDS used for description modification reports. This MDS periodically inserts and deletes a VMD including Channels including Metrics.</pm:ConceptDescription>
                </pm:Type>
                <pm:Vmd Handle="vmd_0.mds_1">
                    <pm:Type Code="67108868">
                        <pm:ConceptDescription Lang="en-US">SDPi Test VMD that contains a metric and an alarm for which units and cause-remedy information is periodically updated (description updates)</pm:ConceptDescription>
                    </pm:Type>
                    <pm:AlertSystem Handle="alert_system.vmd_0.mds_1">
                        <pm:AlertCondition Kind="Oth" Priority="None" Handle="alert_condition_0.vmd_0.mds_1">
                            <pm:Type Code="67108885">
<pm:ConceptDescription Lang="en-US">An alert condition that periodically changes its cause-remedy information at least every 5 seconds</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Source>numeric_metric_0.channel_0.vmd_0.mds_1</pm:Source>
                        </pm:AlertCondition>
                    </pm:AlertSystem>
                    <pm:Channel Handle="channel_0.vmd_0.mds_1">
                        <pm:Type Code="67108873">
                            <pm:ConceptDescription Lang="en-US">Channel that contains a metric which is periodically changing its unit of measure</pm:ConceptDescription>
                        </pm:Type>
                        <pm:Metric xsi:type="pm:NumericMetricDescriptor" Resolution="1" MetricCategory="Set" MetricAvailability="Intr" Handle="numeric_metric_0.channel_0.vmd_0.mds_1">
                            <pm:Type Code="157784">
<pm:ConceptDescription Lang="en-US">Flow Rate: Numeric measurement that periodically changes the unit of measure at least every 5 seconds</pm:ConceptDescription>
                            </pm:Type>
                            <pm:Unit Code="265266"/>
                        </pm:Metric>
                    </pm:Channel>
                </pm:Vmd>
            </pm:Mds>
        </pm:MdDescription>
        <pm:MdState>
            <pm:State xsi:type="pm:LimitAlertConditionState" MonitoredAlertLimits="None" ActivationState="On" DescriptorHandle="alert_condition_0.mds_0">
                <pm:Limits/>
            </pm:State>
            <pm:State xsi:type="pm:AlertSignalState" ActivationState="On" DescriptorHandle="alert_signal_0.mds_0"/>
            <pm:State xsi:type="pm:AlertSystemState" ActivationState="On" DescriptorHandle="alert_system.mds_0"/>
            <pm:State xsi:type="pm:SetContextStateOperationState" OperatingMode="En" DescriptorHandle="set_context_0.sco.mds_0"/>
            <pm:State xsi:type="pm:SetValueOperationState" OperatingMode="En" DescriptorHandle="set_value_0.sco.mds_0"/>
            <pm:State xsi:type="pm:SetStringOperationState" OperatingMode="En" DescriptorHandle="set_string_0.sco.mds_0"/>
            <pm:State xsi:type="pm:ActivateOperationState" OperatingMode="En" DescriptorHandle="activate_0.sco.mds_0"/>
            <pm:State xsi:type="pm:ActivateOperationState" OperatingMode="En" DescriptorHandle="activate_1.sco.mds_0"/>
            <pm:State xsi:type="pm:ScoState" ActivationState="On" DescriptorHandle="sco.mds_0"/>
            <pm:State xsi:type="pm:SystemContextState" ActivationState="On" DescriptorHandle="system_context.mds_0"/>
            <pm:State xsi:type="pm:NumericMetricState" ActivationState="On" DescriptorHandle="numeric_metric_0.channel_0.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:EnumStringMetricState" ActivationState="On" DescriptorHandle="enum_string_metric_0.channel_0.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:StringMetricState" ActivationState="On" DescriptorHandle="string_metric_0.channel_0.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:StringMetricState" ActivationState="On" DescriptorHandle="string_metric_1.channel_0.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:ChannelState" ActivationState="On" DescriptorHandle="channel_0.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:NumericMetricState" ActivationState="On" DescriptorHandle="numeric_metric_1.channel_1.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:RealTimeSampleArrayMetricState" ActivationState="On" DescriptorHandle="rtsa_metric_0.channel_1.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:RealTimeSampleArrayMetricState" ActivationState="On" DescriptorHandle="rtsa_metric_1.channel_1.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:RealTimeSampleArrayMetricState" ActivationState="On" DescriptorHandle="rtsa_metric_2.channel_1.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:RealTimeSampleArrayMetricState" ActivationState="On" DescriptorHandle="rtsa_metric_3.channel_1.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:ChannelState" ActivationState="On" DescriptorHandle="channel_1.vmd_0.mds_0"/>
            <pm:State xsi:type="pm:VmdState" ActivationState="On" DescriptorHandle="vmd_0.mds_0"/>
            <pm:State xsi:type="pm:SetMetricStateOperationState" OperatingMode="En" DescriptorHandle="set_metric_0.sco.vmd_1.mds_0"/>
            <pm:State xsi:type="pm:ScoState" ActivationState="On" DescriptorHandle="sco.vmd1.mds_0"/>
            <pm:State xsi:type="pm:NumericMetricState" ActivationState="On" DescriptorHandle="numeric_metric_0.channel_0.vmd_1.mds_0"/>
            <pm:State xsi:type="pm:NumericMetricState" ActivationState="On" DescriptorHandle="numeric_metric_1.channel_0.vmd_1.mds_0"/>
            <pm:State xsi:type="pm:ChannelState" ActivationState="On" DescriptorHandle="channel_0.vmd_1.mds_0"/>
            <pm:State xsi:type="pm:VmdState" ActivationState="On" DescriptorHandle="vmd_1.mds_0"/>
            <pm:State xsi:type="pm:ClockState" RemoteSync="false" ActivationState="On" DescriptorHandle="clock.mds_0"/>
            <pm:State xsi:type="pm:BatteryState" ActivationState="On" DescriptorHandle="battery_0.mds_0"/>
            <pm:State xsi:type="pm:MdsState" ActivationState="On" DescriptorHandle="mds_0"/>
            <pm:State xsi:type="pm:AlertConditionState" ActivationState="On" DescriptorHandle="alert_condition_0.vmd_0.mds_1"/>
            <pm:State xsi:type="pm:AlertSystemState" ActivationState="On" DescriptorHandle="alert_system.vmd_0.mds_1"/>
            <pm:State xsi:type="pm:NumericMetricState" ActivationState="On" DescriptorHandle="numeric_metric_0.channel_0.vmd_0.mds_1"/>
            <pm:State xsi:type="pm:ChannelState" ActivationState="On" DescriptorHandle="channel_0.vmd_0.mds_1"/>
            <pm:State xsi:type="pm:VmdState" ActivationState="On" DescriptorHandle="vmd_0.mds_1"/>
            <pm:State xsi:type="pm:MdsState" ActivationState="On" DescriptorHandle="mds_1"/>
        </pm:MdState>
    </msg:Mdib>
</msg:GetMdibResponse>""")  # noqa: E501

    schema = schema_resolver.mk_schema_validator(list(namespaces.PrefixesEnum), namespaces.default_ns_helper)
    schema.assertValid(get_mdib_response)
