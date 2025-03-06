"""Implementation of reference provider.

The reference provider gets its parameters from environment variables:
- adapter_ip specifies which ip address shall be used
- ca_folder specifies where the communication certificates are located.
- ref_fac, ref_poc and ref_bed specify the location values facility, point of care and bed.
- ssl_passwd specifies an optional password for the certificates.

If a value is not provided as environment variable, the default value (see code below) will be used.
"""

from __future__ import annotations

import datetime
import json
import logging.config
import os
import pathlib
import traceback
import uuid
from decimal import Decimal
from time import sleep
from typing import TYPE_CHECKING

import sdc11073
from sdc11073 import location, network
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.loghelper import LoggerAdapter
from sdc11073.mdib import ProviderMdib, descriptorcontainers
from sdc11073.provider import SdcProvider, components
from sdc11073.provider.servicesfactory import DPWSHostedService, HostedServices, mk_dpws_hosts
from sdc11073.provider.subscriptionmgr_async import SubscriptionsManagerReferenceParamAsync
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType

if TYPE_CHECKING:
    from sdc11073.provider.components import SdcProviderComponents


def get_network_adapter() -> network.NetworkAdapter:
    """Get network adapter from environment or first loopback."""
    if (ip := os.getenv('ref_ip')) is not None:  # noqa: SIM112
        return network.get_adapter_containing_ip(ip)
    # get next available loopback adapter
    return next(adapter for adapter in network.get_adapters() if adapter.is_loopback)


def get_location() -> location.SdcLocation:
    """Get location from environment or default."""
    return location.SdcLocation(
        fac=os.getenv('ref_fac', default='r_fac'),  # noqa: SIM112
        poc=os.getenv('ref_poc', default='r_poc'),  # noqa: SIM112
        bed=os.getenv('ref_bed', default='r_bed'),  # noqa: SIM112
    )


def get_ssl_context() -> sdc11073.certloader.SSLContextContainer | None:
    """Get ssl context from environment or None."""
    if (ca_folder := os.getenv('ref_ca')) is None:  # noqa: SIM112
        return None
    return mk_ssl_contexts_from_folder(
        ca_folder,
        private_key='user_private_key_encrypted.pem',
        certificate='user_certificate_root_signed.pem',
        ca_public_key='root_certificate.pem',
        cyphers_file=None,
        ssl_passwd=os.getenv('ref_ssl_passwd'),  # noqa: SIM112
    )


def get_epr() -> uuid.UUID:
    """Get epr from environment or default."""
    if (epr := os.getenv('ref_search_epr')) is not None:  # noqa: SIM112
        return uuid.UUID(epr)
    return uuid.UUID('12345678-6f55-11ea-9697-123456789abc')


def get_mdib_path() -> pathlib.Path:
    """Get mdib from environment or default mdib."""
    if mdib_path := os.getenv('ref_mdib'):  # noqa:SIM112
        return pathlib.Path(mdib_path)
    return pathlib.Path(__file__).parent.joinpath('PlugathonMdibV2.xml')


numeric_metric_handle = 'numeric_metric_0.channel_0.vmd_0.mds_0'
string_metric_handle = 'string_metric_0.channel_0.vmd_0.mds_0'
alert_condition_handle = 'alert_condition_0.vmd_0.mds_1'
alert_signal_handle = 'alert_signal_0.mds_0'
set_value_handle = 'set_value_0.sco.mds_0'
set_string_handle = 'set_string_0.sco.mds_0'
battery_handle = 'battery_0.mds_0'
vmd_handle = 'vmd_0.mds_0'
mds_handle = 'mds_0'
USE_REFERENCE_PARAMETERS = False

# some switches to enable/disable some of the provider data updates
# enabling allows to verify that the reference consumer detects missing updates

# 4 State Reports
# a) The Reference Provider produces at least 5 numeric metric updates in 30 seconds
# b) The Reference Provider produces at least 5 string metric updates (StringMetric or EnumStringMetric) in 30 seconds
# c) The Reference Provider produces at least 5 alert condition updates (AlertCondition or LimitAlertCondition)
#    in 30 seconds
# d) The Reference Provider produces at least 5 alert signal updates in 30 seconds
# e) The Reference Provider provides alert system self checks in accordance to the periodicity defined in the
#    MDIB (at least every 10 seconds)
# f) The Reference Provider provides 3 waveforms (RealTimeSampleArrayMetric) x 10 messages per second x 100 samples
#    per message
# g) The Reference Provider provides changes for the following components:
#   * At least 5 Clock or Battery object updates in 30 seconds (Component report)
#   * At least 5 MDS or VMD updates in 30 seconds (Component report)
# g) The Reference Provider provides changes for the following operational states:
#    At least 5 Operation updates in 30 seconds; enable/disable operations; some different than the ones mentioned
#    above (Operational State Report)"""
enable_4a = True
enable_4b = True
enable_4c = True
enable_4d = True
# switching 4e not implemented
enable_4f = True

# 5 Description Modifications:
# a) The Reference Provider produces at least 1 update every 10 seconds comprising
#     * Update Alert condition concept description of Type
#     * Update Alert condition cause-remedy information
#     * Update Unit of measure (metrics)
enable_5a1 = True
enable_5a2 = True
enable_5a3 = True

# 6 Operation invocation
# a) (removed)
# b) SetContextState:
#     * Payload: 1 Patient Context
#     * Context state is added to the MDIB including context association and validation
#     * If there is an associated context already, that context shall be disassociated
#         * Handle and version information is generated by the provider
#     * In order to avoid infinite growth of patient contexts, older contexts are allowed to be removed from the MDIB
#       (=ContextAssociation=No)
# c) SetValue: Immediately answers with "finished"
#     * Finished has to be sent as a report in addition to the response =>
# d) SetString: Initiates a transaction that sends Wait, Start and Finished
# e) SetMetricStates:
#     * Payload: 2 Metric States (settings; consider alert limits)
#     * Immediately sends finished
#     * Action: Alter values of metrics
enable_6c = True
enable_6d = True
enable_6e = True


def mk_all_services_except_localization(
    sdc_provider: SdcProvider,
    components: SdcProviderComponents,
    subscription_managers: dict,
) -> HostedServices:
    """Create all services except localization service."""
    # register all services with their endpoint references acc. to structure in components
    dpws_services, services_by_name = mk_dpws_hosts(sdc_provider, components, DPWSHostedService, subscription_managers)
    return HostedServices(
        dpws_services,
        services_by_name['GetService'],
        set_service=services_by_name.get('SetService'),
        context_service=services_by_name.get('ContextService'),
        description_event_service=services_by_name.get('DescriptionEventService'),
        state_event_service=services_by_name.get('StateEventService'),
        waveform_service=services_by_name.get('WaveformService'),
        containment_tree_service=services_by_name.get('ContainmentTreeService'),
        # localization_service=services_by_name.get('LocalizationService')  # noqa: ERA001
    )


def provide_realtime_data(sdc_provider: SdcProvider):
    """Provide realtime data."""
    waveform_provider = sdc_provider.waveform_provider
    if waveform_provider is None:
        return
    mdib_waveforms = sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.RealTimeSampleArrayMetricDescriptor)
    for waveform in mdib_waveforms:
        wf_generator = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveform_period=1.1, sample_period=0.001)
        waveform_provider.register_waveform_generator(waveform.Handle, wf_generator)


def run_provider():  # noqa: PLR0915, PLR0912, C901
    """Run provider until KeyboardError is raised."""
    with pathlib.Path(__file__).parent.joinpath('logging_default.json').open() as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    xtra_log_config = os.getenv('ref_xtra_log_cnf')  # noqa:SIM112
    if xtra_log_config is not None:
        with pathlib.Path(xtra_log_config).open() as f:
            logging_setup2 = json.load(f)
            logging.config.dictConfig(logging_setup2)

    logger = logging.getLogger('sdc')
    logger = LoggerAdapter(logger)
    logger.info('%s', 'start')
    adapter_ip = get_network_adapter().ip
    wsd = WSDiscovery(adapter_ip)
    wsd.start()
    my_mdib = ProviderMdib.from_mdib_file(str(get_mdib_path()))
    my_uuid = get_epr()
    print(f'UUID for this device is {my_uuid}')
    loc = get_location()
    print(f'location for this device is {loc}')
    dpws_model = ThisModelType(
        manufacturer='sdc11073',
        manufacturer_url='www.sdc11073.com',
        model_name='TestDevice',
        model_number='1.0',
        model_url='www.sdc11073.com/model',
        presentation_url='www.sdc11073.com/model/presentation',
    )

    dpws_device = ThisDeviceType(friendly_name='TestDevice', firmware_version='Version1', serial_number='12345')
    ssl_context = get_ssl_context()
    if USE_REFERENCE_PARAMETERS:
        tmp = {'StateEvent': SubscriptionsManagerReferenceParamAsync}
        specific_components = components.SdcProviderComponents(
            subscriptions_manager_class=tmp,
            hosted_services={
                'Get': [components.GetService],
                'StateEvent': [
                    components.StateEventService,
                    components.ContextService,
                    components.DescriptionEventService,
                    components.WaveformService,
                ],
                'Set': [components.SetService],
                'ContainmentTree': [components.ContainmentTreeService],
            },
            soap_client_class=SoapClientAsync,
        )
    else:
        specific_components = components.SdcProviderComponents(
            hosted_services={
                'Get': [components.GetService],
                'StateEvent': [
                    components.StateEventService,
                    components.ContextService,
                    components.DescriptionEventService,
                    components.WaveformService,
                ],
                'Set': [components.SetService],
                'ContainmentTree': [components.ContainmentTreeService],
            },
        )
    sdc_provider = SdcProvider(
        wsd,
        dpws_model,
        dpws_device,
        my_mdib,
        my_uuid,
        ssl_context_container=ssl_context,
        specific_components=specific_components,
        max_subscription_duration=15,
    )
    sdc_provider.start_all()

    # disable delayed processing for 2 operations
    if enable_6c:
        sdc_provider.get_operation_by_handle('set_value_0.sco.mds_0').delayed_processing = False
    if not enable_6d:
        sdc_provider.get_operation_by_handle('set_string_0.sco.mds_0').delayed_processing = False
    if enable_6e:
        sdc_provider.get_operation_by_handle('set_metric_0.sco.vmd_1.mds_0').delayed_processing = False

    pm = my_mdib.data_model.pm_names
    pm_types = my_mdib.data_model.pm_types
    validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
    sdc_provider.set_location(loc, validators)
    if enable_4f:
        provide_realtime_data(sdc_provider)
    patient_descriptor_handle = my_mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor)[0].Handle
    with my_mdib.context_state_transaction() as mgr:
        patient_container = mgr.mk_context_state(patient_descriptor_handle)
        patient_container.CoreData.Givenname = 'Given'
        patient_container.CoreData.Middlename = ['Middle']
        patient_container.CoreData.Familyname = 'Familiy'
        patient_container.CoreData.Birthname = 'Birthname'
        patient_container.CoreData.Title = 'Title'
        patient_container.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        patient_container.Validator.extend(validators)
        identifiers = []
        patient_container.Identification = identifiers

    all_descriptors = list(sdc_provider.mdib.descriptions.objects)
    all_descriptors.sort(key=lambda x: x.Handle)
    numeric_metric = None
    string_metric = None
    alert_condition = None
    alert_signal = None
    battery_descriptor = None
    string_operation = None
    value_operation = None

    # search for descriptors of specific types
    for one_descriptor in all_descriptors:
        if one_descriptor.Handle == numeric_metric_handle:
            numeric_metric = one_descriptor
        if one_descriptor.Handle == string_metric_handle:
            string_metric = one_descriptor
        if one_descriptor.Handle == alert_condition_handle:
            alert_condition = one_descriptor
        if one_descriptor.Handle == alert_signal_handle:
            alert_signal = one_descriptor
        if one_descriptor.Handle == battery_handle:
            battery_descriptor = one_descriptor
        if one_descriptor.Handle == set_value_handle:
            value_operation = one_descriptor
        if one_descriptor.Handle == set_string_handle:
            string_operation = one_descriptor

    with sdc_provider.mdib.metric_state_transaction() as mgr:
        state = mgr.get_state(value_operation.OperationTarget)
        if not state.MetricValue:
            state.mk_metric_value()
        state = mgr.get_state(string_operation.OperationTarget)
        if not state.MetricValue:
            state.mk_metric_value()
    print('Running forever, CTRL-C to  exit')
    try:
        str_current_value = 0
        while True:
            if numeric_metric:
                try:
                    if enable_4a:
                        with sdc_provider.mdib.metric_state_transaction() as mgr:
                            state = mgr.get_state(numeric_metric.Handle)
                            if not state.MetricValue:
                                state.mk_metric_value()
                            if state.MetricValue.Value is None:
                                state.MetricValue.Value = Decimal('0')
                            else:
                                state.MetricValue.Value += Decimal(1)
                    if enable_5a3:
                        with sdc_provider.mdib.descriptor_transaction() as mgr:
                            descriptor: descriptorcontainers.AbstractMetricDescriptorContainer = mgr.get_descriptor(
                                numeric_metric.Handle,
                            )
                            descriptor.Unit.Code = 'code1' if descriptor.Unit.Code == 'code2' else 'code2'
                except Exception:  # noqa: BLE001
                    print(traceback.format_exc())
            else:
                print('Numeric Metric not found in MDIB!')
            if string_metric:
                try:
                    if enable_4b:
                        with sdc_provider.mdib.metric_state_transaction() as mgr:
                            state = mgr.get_state(string_metric.Handle)
                            if not state.MetricValue:
                                state.mk_metric_value()
                            state.MetricValue.Value = f'my string {str_current_value}'
                            str_current_value += 1
                except Exception:  # noqa: BLE001
                    print(traceback.format_exc())
            else:
                print('Numeric Metric not found in MDIB!')

            if alert_condition:
                try:
                    if enable_4c:
                        with sdc_provider.mdib.alert_state_transaction() as mgr:
                            state = mgr.get_state(alert_condition.Handle)
                            state.Presence = not state.Presence
                except Exception:  # noqa: BLE001
                    print(traceback.format_exc())
                try:
                    with sdc_provider.mdib.descriptor_transaction() as mgr:
                        now = datetime.datetime.now(tz=datetime.UTC)
                        text = f'last changed at {now.hour:02d}:{now.minute:02d}:{now.second:02d}'
                        descriptor: descriptorcontainers.AlertConditionDescriptorContainer = mgr.get_descriptor(
                            alert_condition.Handle,
                        )
                        if enable_5a1:
                            if len(descriptor.Type.ConceptDescription) == 0:
                                descriptor.Type.ConceptDescription.append(pm_types.LocalizedText(text))
                            else:
                                descriptor.Type.ConceptDescription[0].text = text
                        if enable_5a2:
                            if len(descriptor.CauseInfo) == 0:
                                cause_info = pm_types.CauseInfo()
                                cause_info.RemedyInfo = pm_types.RemedyInfo()
                                descriptor.CauseInfo.append(cause_info)
                            if len(descriptor.CauseInfo[0].RemedyInfo.Description) == 0:
                                descriptor.CauseInfo[0].RemedyInfo.Description.append(pm_types.LocalizedText(text))
                            else:
                                descriptor.CauseInfo[0].RemedyInfo.Description[0].text = text
                except Exception:  # noqa: BLE001
                    print(traceback.format_exc())

            else:
                print('Alert condition not found in MDIB')

            if alert_signal:
                try:
                    if enable_4d:
                        with sdc_provider.mdib.alert_state_transaction() as mgr:
                            state = mgr.get_state(alert_signal.Handle)
                            if state.Slot is None:
                                state.Slot = 1
                            else:
                                state.Slot += 1
                except Exception:  # noqa:BLE001
                    print(traceback.format_exc())
            else:
                print('Alert signal not found in MDIB')

            if battery_descriptor:
                try:
                    with sdc_provider.mdib.component_state_transaction() as mgr:
                        state = mgr.get_state(battery_descriptor.Handle)
                        if state.Voltage is None:
                            state.Voltage = pm_types.Measurement(value=Decimal('14.4'), unit=pm_types.CodedValue('xyz'))
                        else:
                            state.Voltage.MeasuredValue += Decimal('0.1')
                        print(f'battery voltage = {state.Voltage.MeasuredValue}')
                except Exception:  # noqa:BLE001
                    print(traceback.format_exc())
            else:
                print('battery state not found in MDIB')

            try:
                with sdc_provider.mdib.component_state_transaction() as mgr:
                    state = mgr.get_state(vmd_handle)
                    state.OperatingHours = 2 if state.OperatingHours != 2 else 1  # noqa:PLR2004
                    print(f'operating hours = {state.OperatingHours}')
            except Exception:  # noqa:BLE001
                print(traceback.format_exc())

            try:
                with sdc_provider.mdib.component_state_transaction() as mgr:
                    state = mgr.get_state(mds_handle)
                    state.Lang = 'de' if state.Lang != 'de' else 'en'
                    print(f'mds lang = {state.Lang}')
            except Exception:  # noqa:BLE001
                print(traceback.format_exc())

            # add or rm vmd
            add_rm_metric_handle = 'add_rm_metric'
            add_rm_channel_handle = 'add_rm_channel'
            add_rm_vmd_handle = 'add_rm_vmd'
            add_rm_mds_handle = 'mds_0'
            vmd_descriptor = sdc_provider.mdib.descriptions.handle.get_one(add_rm_vmd_handle, allow_none=True)
            if vmd_descriptor is None:
                vmd = descriptorcontainers.VmdDescriptorContainer(add_rm_vmd_handle, add_rm_mds_handle)
                channel = descriptorcontainers.ChannelDescriptorContainer(add_rm_channel_handle, add_rm_vmd_handle)
                metric = descriptorcontainers.StringMetricDescriptorContainer(
                    add_rm_metric_handle,
                    add_rm_channel_handle,
                )
                metric.Unit = pm_types.CodedValue('123')
                with sdc_provider.mdib.descriptor_transaction() as mgr:
                    mgr.add_descriptor(vmd)
                    mgr.add_descriptor(channel)
                    mgr.add_descriptor(metric)
                    mgr.add_state(sdc_provider.mdib.data_model.mk_state_container(vmd))
                    mgr.add_state(sdc_provider.mdib.data_model.mk_state_container(channel))
                    mgr.add_state(sdc_provider.mdib.data_model.mk_state_container(metric))
            else:
                with sdc_provider.mdib.descriptor_transaction() as mgr:
                    mgr.remove_descriptor(add_rm_vmd_handle)

            # enable disable operation
            with sdc_provider.mdib.operational_state_transaction() as mgr:
                op_state = mgr.get_state('activate_0.sco.mds_0')
                op_state.OperatingMode = (
                    pm_types.OperatingMode.ENABLED
                    if op_state.OperatingMode == pm_types.OperatingMode.ENABLED
                    else pm_types.OperatingMode.DISABLED
                )
                print(f'operation activate_0.sco.mds_0 {op_state.OperatingMode}')

            sleep(5)
    except KeyboardInterrupt:
        print('Exiting...')


if __name__ == '__main__':
    run_provider()
