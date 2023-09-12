from __future__ import annotations
import json
import logging.config
import os
import traceback
from decimal import Decimal
from time import sleep
from uuid import UUID
import datetime
from typing import TYPE_CHECKING

from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.location import SdcLocation
from sdc11073.loghelper import LoggerAdapter
from sdc11073.mdib import ProviderMdib, descriptorcontainers
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from sdc11073.provider import components
from sdc11073.provider import SdcProvider
from sdc11073.provider.servicesfactory import DPWSHostedService
from sdc11073.provider.servicesfactory import HostedServices, mk_dpws_hosts
from sdc11073.provider.subscriptionmgr_async import SubscriptionsManagerReferenceParamAsync
from sdc11073.provider import waveforms
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_types, pm_qnames
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType

here = os.path.dirname(__file__)
default_mdib_path = os.path.join(here, 'mdib_test_sequence_2_v4(temp).xml')
mdib_path = os.getenv('ref_mdib') or default_mdib_path
xtra_log_config = os.getenv('ref_xtra_log_cnf')  # or None

My_UUID_str = '12345678-6f55-11ea-9697-123456789bcd'

# these variables define how the device is published on the network:
adapter_ip = os.getenv('ref_ip') or '127.0.0.1'
ca_folder = os.getenv('ref_ca')
ref_fac = os.getenv('ref_fac') or 'r_fac'
ref_poc = os.getenv('ref_poc') or 'r_poc'
ref_bed = os.getenv('ref_bed') or 'r_bed'
ssl_passwd = os.getenv('ref_ssl_passwd') or None

numeric_metric_handle = "numeric_metric_0.channel_0.vmd_0.mds_0"
string_metric_handle = "string_metric_0.channel_0.vmd_0.mds_0"
alert_condition_handle = "alert_condition_0.vmd_0.mds_1"
alert_signal_handle = "alert_signal_0.mds_0"
set_value_handle = "set_value_0.sco.mds_0"
set_string_handle = "set_string_0.sco.mds_0"
battery_handle = 'battery_0.mds_0'
vmd_handle = "vmd_0.mds_0"
mds_handle = "mds_0"
USE_REFERENCE_PARAMETERS = False


def mk_all_services_except_localization(sdc_provider, components, subscription_managers) -> HostedServices:
    # register all services with their endpoint references acc. to structure in components
    dpws_services, services_by_name = mk_dpws_hosts(sdc_provider, components, DPWSHostedService, subscription_managers)
    hosted_services = HostedServices(dpws_services,
                                     services_by_name['GetService'],
                                     set_service=services_by_name.get('SetService'),
                                     context_service=services_by_name.get('ContextService'),
                                     description_event_service=services_by_name.get('DescriptionEventService'),
                                     state_event_service=services_by_name.get('StateEventService'),
                                     waveform_service=services_by_name.get('WaveformService'),
                                     containment_tree_service=services_by_name.get('ContainmentTreeService'),
                                     # localization_service=services_by_name.get('LocalizationService')
                                     )
    return hosted_services


def provide_realtime_data(sdc_provider):
    waveform_provider = sdc_provider.mdib.xtra.waveform_provider
    if waveform_provider is None:
        return
    mdib_waveforms = sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.RealTimeSampleArrayMetricDescriptor)
    for waveform in mdib_waveforms:
        wf_generator = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1, sampleperiod=0.001)
        waveform_provider.register_waveform_generator(waveform.Handle, wf_generator)


if __name__ == '__main__':
    with open(os.path.join(here, 'logging_default.jsn')) as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    if xtra_log_config is not None:
        with open(xtra_log_config) as f:
            logging_setup2 = json.load(f)
            logging.config.dictConfig(logging_setup2)

    logger = logging.getLogger('sdc')
    logger = LoggerAdapter(logger)
    logger.info('{}', 'start')
    wsd = WSDiscovery(adapter_ip)
    wsd.start()
    my_mdib = ProviderMdib.from_mdib_file(mdib_path)
    my_uuid = UUID(My_UUID_str)
    print("UUID for this device is {}".format(my_uuid))
    loc = SdcLocation(ref_fac, ref_poc, ref_bed)
    print("location for this device is {}".format(loc))
    dpwsModel = ThisModelType(manufacturer='sdc11073',
                              manufacturer_url='www.sdc11073.com',
                              model_name='TestDevice',
                              model_number='1.0',
                              model_url='www.sdc11073.com/model',
                              presentation_url='www.sdc11073.com/model/presentation')

    dpwsDevice = ThisDeviceType(friendly_name='TestDevice',
                                firmware_version='Version1',
                                serial_number='12345')
    if ca_folder:
        ssl_contexts = mk_ssl_contexts_from_folder(ca_folder,
                                                 private_key='user_private_key_encrypted.pem',
                                                 certificate='user_certificate_root_signed.pem',
                                                 ca_public_key='root_certificate.pem',
                                                 cyphers_file=None,
                                                 ssl_passwd=ssl_passwd)
    else:
        ssl_contexts = None
    if USE_REFERENCE_PARAMETERS:
        tmp = {'StateEvent': SubscriptionsManagerReferenceParamAsync}
        specific_components = components.SdcProviderComponents(
            subscriptions_manager_class=tmp,
            hosted_services={'Get': [components.GetService],
                             'StateEvent': [components.StateEventService,
                                            components.ContextService,
                                            components.DescriptionEventService,
                                            components.WaveformService],
                             'Set': [components.SetService],
                             'ContainmentTree': [components.ContainmentTreeService]},
            soap_client_class=SoapClientAsync)
    else:
#        specific_components = SdcProviderComponents(services_factory=mk_all_services_except_localization,
#                                                  soap_client_class=SoapClientAsync)
        specific_components = components.SdcProviderComponents(
            hosted_services={'Get': [components.GetService],
                             'StateEvent': [components.StateEventService,
                                            components.ContextService,
                                            components.DescriptionEventService,
                                            components.WaveformService],
                             'Set': [components.SetService],
                             'ContainmentTree': [components.ContainmentTreeService]})
    sdc_provider = SdcProvider(wsd, dpwsModel, dpwsDevice, my_mdib, my_uuid,
                          ssl_context_container=ssl_contexts,
                          specific_components=specific_components,
                          max_subscription_duration=15
                             )
    sdc_provider.start_all()

    validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
    sdc_provider.set_location(loc, validators)
    provide_realtime_data(sdc_provider)
    pm = my_mdib.data_model.pm_names
    pm_types = my_mdib.data_model.pm_types
    patientDescriptorHandle = my_mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor)[0].Handle
    with my_mdib.transaction_manager() as mgr:
        patientContainer = mgr.mk_context_state(patientDescriptorHandle)
        patientContainer.CoreData.Givenname = "Given"
        patientContainer.CoreData.Middlename = ["Middle"]
        patientContainer.CoreData.Familyname = "Familiy"
        patientContainer.CoreData.Birthname = "Birthname"
        patientContainer.CoreData.Title = "Title"
        patientContainer.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        patientContainer.Validator.extend(validators)
        identifiers = []
        patientContainer.Identification = identifiers

    descs = list(sdc_provider.mdib.descriptions.objects)
    descs.sort(key=lambda x: x.Handle)
    numeric_metric = None
    string_metric = None
    alertCondition = None
    alertSignal = None
    battery_descriptor = None
    activateOperation = None
    stringOperation = None
    valueOperation = None
    for oneContainer in descs:
        if oneContainer.Handle == numeric_metric_handle:
            numeric_metric = oneContainer
        if oneContainer.Handle == string_metric_handle:
            string_metric = oneContainer
        if oneContainer.Handle == alert_condition_handle:
            alertCondition = oneContainer
        if oneContainer.Handle == alert_signal_handle:
            alertSignal = oneContainer
        if oneContainer.Handle == battery_handle:
            battery_descriptor  = oneContainer
        if oneContainer.Handle == set_value_handle:
            valueOperation = oneContainer
        if oneContainer.Handle == set_string_handle:
            stringOperation = oneContainer

    with sdc_provider.mdib.transaction_manager() as mgr:
        state = mgr.get_state(valueOperation.OperationTarget)
        if not state.MetricValue:
            state.mk_metric_value()
        state = mgr.get_state(stringOperation.OperationTarget)
        if not state.MetricValue:
            state.mk_metric_value()
    print("Running forever, CTRL-C to  exit")
    try:
        num_current_value = 0
        str_current_value = 0
        while True:
            if numeric_metric:
                try:
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(numeric_metric.Handle)
                        if not state.MetricValue:
                            state.mk_metric_value()
                        state.MetricValue.Value = Decimal(num_current_value)
                        num_current_value += 1
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        descriptor: descriptorcontainers.AbstractMetricDescriptorContainer = mgr.get_descriptor(numeric_metric.Handle)
                        descriptor.Unit.Code = 'code1' if descriptor.Unit.Code == 'code2' else 'code2'
                except Exception as ex:
                    print(traceback.format_exc())
            else:
                print("Numeric Metric not found in MDIB!")
            if string_metric:
                try:
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(string_metric.Handle)
                        if not state.MetricValue:
                            state.mk_metric_value()
                        state.MetricValue.Value = f'string {str_current_value}'
                        str_current_value += 1
                except Exception as ex:
                    print(traceback.format_exc())
            else:
                print("Numeric Metric not found in MDIB!")

            if alertCondition:
                try:
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(alertCondition.Handle)
                        state.Presence = not state.Presence
                except Exception as ex:
                    print(traceback.format_exc())
                try:
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        now = datetime.datetime.now()
                        text = f'last changed at {now.hour:02d}:{now.minute:02d}:{now.second:02d}'
                        descriptor: descriptorcontainers.AlertConditionDescriptorContainer = mgr.get_descriptor(alertCondition.Handle)
                        if len(descriptor.Type.ConceptDescription) == 0:
                            descriptor.Type.ConceptDescription.append(pm_types.LocalizedText(text))
                        else:
                            descriptor.Type.ConceptDescription[0].text = text
                        if len(descriptor.CauseInfo) == 0:
                            descriptor.CauseInfo.append(pm_types.CauseInfo())
                        if len(descriptor.CauseInfo[0].RemedyInfo.Description) == 0:
                            descriptor.CauseInfo[0].RemedyInfo.Description.append(pm_types.LocalizedText(text))
                        else:
                            descriptor.CauseInfo[0].RemedyInfo.Description[0].text = text
                except Exception as ex:
                    print(traceback.format_exc())

            else:
                print("Alert condition not found in MDIB")

            if alertSignal:
                try:
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(alertSignal.Handle)
                        if state.Slot is None:
                            state.Slot = 1
                        else:
                            state.Slot += 1
                except Exception as ex:
                    print(traceback.format_exc())
            else:
                print("Alert signal not found in MDIB")

            if battery_descriptor:
                try:
                    with sdc_provider.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(battery_descriptor.Handle)
                        if state.Voltage is None:
                            state.Voltage = pm_types.Measurement(value=Decimal('14.4'), unit=pm_types.CodedValue('xyz'))
                        else:
                            state.Voltage.MeasuredValue += Decimal('0.1')
                        print(f'battery voltage = {state.Voltage.MeasuredValue}')
                except Exception as ex:
                    print(traceback.format_exc())
            else:
                print("battery state not found in MDIB")

            try:
                with sdc_provider.mdib.transaction_manager() as mgr:
                    state = mgr.get_state(vmd_handle)
                    state.OperatingHours = 2 if state.OperatingHours != 2 else 1
                    print(f'operating hours = {state.OperatingHours}')
            except Exception as ex:
                print(traceback.format_exc())

            try:
                with sdc_provider.mdib.transaction_manager() as mgr:
                    state = mgr.get_state(mds_handle)
                    state.Lang = 'de' if state.Lang != 'de' else 'en'
                    print(f'mds lang = {state.Lang}')
            except Exception as ex:
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
                metric = descriptorcontainers.StringMetricDescriptorContainer(add_rm_metric_handle, add_rm_channel_handle)
                metric.Unit = pm_types.CodedValue('123')
                with sdc_provider.mdib.transaction_manager() as mgr:
                    mgr.add_descriptor(vmd)
                    mgr.add_descriptor(channel)
                    mgr.add_descriptor(metric)
                    mgr.add_state(sdc_provider.mdib.data_model.mk_state_container(vmd))
                    mgr.add_state(sdc_provider.mdib.data_model.mk_state_container(channel))
                    mgr.add_state(sdc_provider.mdib.data_model.mk_state_container(metric))
            else:
                with sdc_provider.mdib.transaction_manager() as mgr:
                    mgr.remove_descriptor(add_rm_vmd_handle)

            sleep(5)
    except KeyboardInterrupt:
        print("Exiting...")
