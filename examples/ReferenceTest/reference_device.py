
import logging
import json
import logging.config
import os
from time import sleep
from uuid import UUID

import sdc11073
from sdc11073.loghelper import LoggerAdapter
from sdc11073.namespaces import domTag
from sdc11073.certloader import mk_ssl_context_from_folder

here = os.path.dirname(__file__)
default_mdib_path = os.path.join(here, 'reference_mdib.xml')
mdib_path = os.getenv('ref_mdib') or default_mdib_path
xtra_log_config = os.getenv('ref_xtra_log_cnf') # or None
ca_folder = os.getenv('ref_ca')  # or None

My_UUID_str = '12345678-6f55-11ea-9697-123456789abc'

# these variables define how the device is published on the network:
adapter_ip = os.getenv('ref_ip') or '127.0.0.1'
ref_fac = os.getenv('ref_fac') or 'r_fac'
ref_poc = os.getenv('ref_poc') or 'r_poc'
ref_bed = os.getenv('ref_bed') or 'r_bed'
ssl_passwd = os.getenv('ref_ssl_passwd') or None

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
    dpwsModel = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='sdc11073',
                                                           manufacturer_url='www.sdc11073.com',
                                                           model_name='TestDevice',
                                                           model_number='1.0',
                                                           model_url='www.sdc11073.com/model',
                                                           presentation_url='www.sdc11073.com/model/presentation')

    dpwsDevice = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendly_name='TestDevice',
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
    sdcDevice = sdc11073.sdcdevice.sdcdeviceimpl.SdcDevice(wsd, dpwsModel, dpwsDevice, my_mdib, my_uuid,
                                                           ssl_context=ssl_context)
    sdcDevice.start_all()

    validators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extension_string='System')]
    sdcDevice.set_location(loc, validators)
    patientDescriptorHandle = my_mdib.descriptions.NODETYPE.get(domTag('PatientContextDescriptor'))[0].handle
    with my_mdib.mdibUpdateTransaction() as mgr:
        patientContainer = mgr.get_state(patientDescriptorHandle)
        patientContainer.CoreData.Givenname = "Given"
        patientContainer.CoreData.Middlename = ["Middle"]
        patientContainer.CoreData.Familyname = "Familiy"
        patientContainer.CoreData.Birthname = "Birthname"
        patientContainer.CoreData.Title = "Title"
        patientContainer.ContextAssociation = "Assoc"
        identifiers = []
        patientContainer.Identification = identifiers

    descs = list(sdcDevice.mdib.descriptions.objects)
    descs.sort(key=lambda x: x.handle)
    metric = None
    alertCondition = None
    alertSignal = None
    activateOperation = None
    stringOperation = None
    valueOperation = None
    for oneContainer in descs:
        if oneContainer.handle == "numeric.ch1.vmd0":
            metric = oneContainer
        if oneContainer.handle == "ac0.mds0":
            alertCondition = oneContainer
        if oneContainer.handle == "numeric.ch0.vmd1_sco_0":
            valueOperation = oneContainer
        if oneContainer.handle == "enumstring.ch0.vmd1_sco_0":
            stringOperation = oneContainer
    with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
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
                with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                    state = mgr.get_state(metric.handle)
                    if not state.MetricValue:
                        state.mk_metric_value()
                    state.MetricValue.Value = currentValue
                    currentValue += 1
            else:
                print("Metric not found in MDIB!")
            if alertCondition:
                with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                    state = mgr.get_state(alertCondition.handle)
                    state.Presence = not state.Presence
            else:
                print("Alert not found in MDIB")
            sleep(5)
    except KeyboardInterrupt:
        print("Exiting...")
