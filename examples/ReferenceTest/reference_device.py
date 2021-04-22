
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
    my_mdib = sdc11073.mdib.DeviceMdibContainer.fromMdibFile(mdib_path)
    my_uuid = UUID(My_UUID_str)
    print("UUID for this device is {}".format(my_uuid))
    loc = sdc11073.location.SdcLocation(ref_fac, ref_poc, ref_bed)
    print("location for this device is {}".format(loc))
    dpwsModel = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='sdc11073',
                                                           manufacturerUrl='www.sdc11073.com',
                                                           modelName='TestDevice',
                                                           modelNumber='1.0',
                                                           modelUrl='www.sdc11073.com/model',
                                                           presentationUrl='www.sdc11073.com/model/presentation')

    dpwsDevice = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendlyName='TestDevice',
                                                             firmwareVersion='Version1',
                                                             serialNumber='12345')
    if ca_folder:
        ssl_context = mk_ssl_context_from_folder(ca_folder, cyphers_file=None,
                                                 ssl_passwd=ssl_passwd)
    else:
        ssl_context = None
    sdcDevice = sdc11073.sdcdevice.sdcdeviceimpl.SdcDevice(wsd, my_uuid, dpwsModel, dpwsDevice, my_mdib,
                                                           sslContext=ssl_context)
    sdcDevice.startAll()

    validators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extensionString='System')]
    sdcDevice.setLocation(loc, validators)
    patientDescriptorHandle = my_mdib.descriptions.nodeName.get(domTag('PatientContext'))[0].handle
    with my_mdib.mdibUpdateTransaction() as mgr:
        patientContainer = mgr.getContextState(patientDescriptorHandle)
        patientContainer.Givenname = "Given"
        patientContainer.Middlename = "Middle"
        patientContainer.Familyname = "Familiy"
        patientContainer.Birthname = "Birthname"
        patientContainer.Title = "Title"
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
        state = mgr.getMetricState(valueOperation.OperationTarget)
        if not state.metricValue:
            state.mkMetricValue()
        state = mgr.getMetricState(stringOperation.OperationTarget)
        if not state.metricValue:
            state.mkMetricValue()
    print("Running forever, CTRL-C to  exit")
    try:
        currentValue = 0
        while True:
            if metric:
                with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                    state = mgr.getMetricState(metric.handle)
                    if not state.metricValue:
                        state.mkMetricValue()
                    state.metricValue.Value = currentValue
                    currentValue += 1
            else:
                print("Metric not found in MDIB!")
            if alertCondition:
                with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                    state = mgr.getAlertState(alertCondition.handle)
                    state.Presence = not state.Presence
            else:
                print("Alert not found in MDIB")
            sleep(5)
    except KeyboardInterrupt:
        print("Exiting...")
