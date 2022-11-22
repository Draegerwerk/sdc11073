import json
import logging.config
import os
import traceback
from time import sleep
from uuid import UUID
from decimal import Decimal

import sdc11073
from sdc11073.certloader import mk_ssl_context_from_folder
from sdc11073.dpws import ThisDeviceType, ThisModelType
from sdc11073.loghelper import LoggerAdapter
from sdc11073.sdcdevice.components import SdcDeviceComponents
from sdc11073.sdcdevice.subscriptionmgr_async import SubscriptionsManagerReferenceParamAsync
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from sdc11073.sdcdevice.hostedserviceimpl import DPWSHostedService
from sdc11073.sdcdevice.sdc_handlers import HostedServices

here = os.path.dirname(__file__)
default_mdib_path = os.path.join(here, 'reference_mdib.xml')
mdib_path = os.getenv('ref_mdib') or default_mdib_path
xtra_log_config = os.getenv('ref_xtra_log_cnf')  # or None
ca_folder = os.getenv('ref_ca')  # or None

My_UUID_str = '12345678-6f55-11ea-9697-123456789abc'

# these variables define how the device is published on the network:
adapter_ip = os.getenv('ref_ip') or '127.0.0.1'
ca_folder = os.getenv('ref_ca')
ref_fac = os.getenv('ref_fac') or 'r_fac'
ref_poc = os.getenv('ref_poc') or 'r_poc'
ref_bed = os.getenv('ref_bed') or 'r_bed'
ssl_passwd = os.getenv('ref_ssl_passwd') or None

USE_REFERENCE_PARAMETERS = False

def mk_all_services_except_localization(sdc_device, components, sdc_definitions) -> HostedServices:
    # register all services with their endpoint references acc. to sdc standard
    actions = sdc_definitions.Actions
    service_handlers_lookup = components.service_handlers
    cls = service_handlers_lookup['GetService']
    get_service = cls('GetService', sdc_device)
    # cls = service_handlers_lookup['LocalizationService']
    # localization_service = cls('LocalizationService', sdc_device)
    offered_subscriptions = []
    get_service_hosted = DPWSHostedService(sdc_device, 'Get',
                                           components.msg_dispatch_method,
                                           [get_service],
                                           offered_subscriptions)

    # grouped acc to sdc REQ 0035
    cls = service_handlers_lookup['ContextService']
    context_service = cls('ContextService', sdc_device)
    cls = service_handlers_lookup['DescriptionEventService']
    description_event_service = cls('DescriptionEventService', sdc_device)
    cls = service_handlers_lookup['StateEventService']
    state_event_service = cls('StateEventService', sdc_device)
    cls = service_handlers_lookup['WaveformService']
    waveform_service = cls('WaveformService', sdc_device)

    offered_subscriptions = [actions.EpisodicContextReport,
                             actions.DescriptionModificationReport,
                             actions.EpisodicMetricReport,
                             actions.EpisodicAlertReport,
                             actions.EpisodicComponentReport,
                             actions.EpisodicOperationalStateReport,
                             actions.Waveform,
                             actions.SystemErrorReport,
                             actions.PeriodicMetricReport,
                             actions.PeriodicAlertReport,
                             actions.PeriodicContextReport,
                             actions.PeriodicComponentReport,
                             actions.PeriodicOperationalStateReport
                             ]

    sdc_service_hosted = DPWSHostedService(sdc_device, 'StateEvent',
                                           components.msg_dispatch_method,
                                           [context_service,
                                            description_event_service,
                                            state_event_service,
                                            waveform_service],
                                           offered_subscriptions)

    cls = service_handlers_lookup['SetService']
    set_dispatcher = cls('SetService', sdc_device)
    offered_subscriptions = [actions.OperationInvokedReport]

    set_service_hosted = DPWSHostedService(sdc_device, 'Set',
                                           components.msg_dispatch_method,
                                           [set_dispatcher],
                                           offered_subscriptions)

    cls = service_handlers_lookup['ContainmentTreeService']
    containment_tree_dispatcher = cls('ContainmentTreeService', sdc_device)
    offered_subscriptions = []
    containment_tree_service_hosted = DPWSHostedService(sdc_device, 'ContainmentTree',
                                                        components.msg_dispatch_method,
                                                        [containment_tree_dispatcher],
                                                        offered_subscriptions)
    dpws_services = (get_service_hosted,
                     sdc_service_hosted,
                     set_service_hosted,
                     containment_tree_service_hosted)
    hosted_services = HostedServices(dpws_services,
                                     get_service,
                                     set_service=set_dispatcher,
                                     context_service=context_service,
                                     description_event_service=description_event_service,
                                     state_event_service=state_event_service,
                                     waveform_service=waveform_service,
                                     containment_tree_service=containment_tree_dispatcher,
#                                      localization_service=localization_service
                                     )
    return hosted_services


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
    wsd = sdc11073.wsdiscovery.WSDiscoveryWhitelist([adapter_ip])
    wsd.start()
    my_mdib = sdc11073.mdib.DeviceMdibContainer.from_mdib_file(mdib_path)
    my_uuid = UUID(My_UUID_str)
    print("UUID for this device is {}".format(my_uuid))
    loc = sdc11073.location.SdcLocation(ref_fac, ref_poc, ref_bed)
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
        ssl_context = mk_ssl_context_from_folder(ca_folder,
                                                 private_key='user_private_key_encrypted.pem',
                                                 certificate='user_certificate_root_signed.pem',
                                                 ca_public_key='root_certificate.pem',
                                                 cyphers_file=None,
                                                 ssl_passwd=ssl_passwd)
    else:
        ssl_context = None
    if USE_REFERENCE_PARAMETERS:
        specific_components = SdcDeviceComponents(subscriptions_manager_class=SubscriptionsManagerReferenceParamAsync,
                                                  services_factory=mk_all_services_except_localization,
                                                  soap_client_class=SoapClientAsync)
    else:
        specific_components = SdcDeviceComponents(services_factory=mk_all_services_except_localization)
    sdcDevice = sdc11073.sdcdevice.sdcdeviceimpl.SdcDevice(wsd, dpwsModel, dpwsDevice, my_mdib, my_uuid,
                                                           ssl_context=ssl_context,
                                                           specific_components=specific_components)
    sdcDevice.start_all()

    validators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extension_string='System')]
    sdcDevice.set_location(loc, validators)
    pm = my_mdib.data_model.pm_names
    pm_types = my_mdib.data_model.pm_types
    #patientDescriptorHandle = my_mdib.descriptions.NODETYPE.get(domTag('PatientContextDescriptor'))[0].handle
    patientDescriptorHandle = my_mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor)[0].Handle
    with my_mdib.transaction_manager() as mgr:
        patientContainer = mgr.mk_context_state(patientDescriptorHandle)
        patientContainer.CoreData.Givenname = "Given"
        patientContainer.CoreData.Middlename = ["Middle"]
        patientContainer.CoreData.Familyname = "Familiy"
        patientContainer.CoreData.Birthname = "Birthname"
        patientContainer.CoreData.Title = "Title"
        patientContainer.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED  #"Assoc"
        identifiers = []
        patientContainer.Identification = identifiers

    descs = list(sdcDevice.mdib.descriptions.objects)
    descs.sort(key=lambda x: x.Handle)
    metric = None
    alertCondition = None
    alertSignal = None
    activateOperation = None
    stringOperation = None
    valueOperation = None
    for oneContainer in descs:
        if oneContainer.Handle == "numeric.ch1.vmd0":
            metric = oneContainer
        if oneContainer.Handle == "ac0.mds0":
            alertCondition = oneContainer
        if oneContainer.Handle == "numeric.ch0.vmd1_sco_0":
            valueOperation = oneContainer
        if oneContainer.Handle == "enumstring.ch0.vmd1_sco_0":
            stringOperation = oneContainer
    with sdcDevice.mdib.transaction_manager() as mgr:
        state = mgr.get_state(valueOperation.OperationTarget)
        if not state.MetricValue:
            state.mk_metric_value()
        state = mgr.get_state(stringOperation.OperationTarget)
        if not state.MetricValue:
            state.mk_metric_value()
    print("Running forever, CTRL-C to  exit")
    try:
        currentValue = 0
        while True:
            if metric:
                try:
                    with sdcDevice.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(metric.Handle)
                        if not state.MetricValue:
                            state.mk_metric_value()
                        state.MetricValue.Value = Decimal(currentValue)
                        currentValue += 1
                except Exception as ex:
                    print(traceback.format_exc())
            else:
                print("Metric not found in MDIB!")
            if alertCondition:
                try:
                    with sdcDevice.mdib.transaction_manager() as mgr:
                        state = mgr.get_state(alertCondition.Handle)
                        state.Presence = not state.Presence
                except Exception as ex:
                    print(traceback.format_exc())
            else:
                print("Alert not found in MDIB")
            sleep(5)
    except KeyboardInterrupt:
        print("Exiting...")
