import logging
import time
import uuid
from sdc11073.xml_types import pm_types, msg_types
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types.actions import periodic_actions
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.consumer import SdcConsumer
from sdc11073.mdib import ConsumerMdib
from sdc11073 import observableproperties
from sdc11073.loghelper import basic_logging_setup
# This example shows how to implement a very simple SDC Consumer (client)
# It will scan for SDC Providers and connect to on well known UUID

# The provider we connect to is known by its UUID
# The UUID is created from a base
baseUUID = uuid.UUID('{cc013678-79f6-403c-998f-3cc0cc050230}')
device_A_UUID = uuid.uuid5(baseUUID, "12345")

# callback function that will be called upon metric updates from the provider
def on_metric_update(metrics_by_handle: dict):
    # we get all changed handles as parameter, iterate over them and output
    print(f"Got update on: {list(metrics_by_handle.keys())}")

def set_ensemble_context(mdib: ConsumerMdib, sdc_consumer: SdcConsumer) -> None:
    # calling operation on remote device 
    print("Trying to set ensemble context of device A")
    # first we get the container to the element in the MDIB
    ensemble_descriptor_container = mdib.descriptions.NODETYPE.getOne(pm.EnsembleContextDescriptor)
    # get the context of our provider(client)
    context_client = sdc_consumer.context_service_client
    # start with empty operation handle and try to find the one we need
    operation_handle = None
    # iterate over all matching handles (can be 0..n)
    for op_descr in mdib.descriptions.NODETYPE.get(pm.SetContextStateOperationDescriptor, []):
        if op_descr.OperationTarget == ensemble_descriptor_container.Handle:
            operation_handle = op_descr.Handle
    # now we should have an operation handle to work with
    # create a new ensemble context as parameter to this operation
    new_ensemble_context = context_client.mk_proposed_context_object(ensemble_descriptor_container.Handle)
    new_ensemble_context.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
    new_ensemble_context.Identification = [
        pm_types.InstanceIdentifier(root="1.2.3", extension_string="SupervisorSuperEnsemble")]
    # execute the remote operation (based on handle) with the newly created ensemble as parameter
    response = context_client.set_context_state(operation_handle, [new_ensemble_context])
    result: msg_types.OperationInvokedReportPart = response.result()
    if result.InvocationInfo.InvocationState not in (msg_types.InvocationState.FINISHED,
                                                     msg_types.InvocationState.FINISHED_MOD):
        print(f'set ensemble context state failed state = {result.InvocationInfo.InvocationState}, '
              f'error = {result.InvocationInfo.InvocationError}, msg = {result.InvocationInfo.InvocationErrorMessage}')
    else:
        print(f'set ensemble context was successful.')


# main entry, will start to scan for the known provider and connect
# runs forever and consumes metrics everafter
if __name__ == '__main__':
    # start with discovery (MDPWS) that is running on the named adapter "Ethernet" (replace as you need it on your machine, e.g. "enet0" or "Ethernet)
    basic_logging_setup(level=logging.INFO)
    my_discovery = WSDiscovery("127.0.0.1")
    # start the discovery
    my_discovery.start()
    # we want to search until we found one device with this client
    found_device = False
    # loop until we found our provider
    while not found_device:
        # we now search explicitly for MedicalDevices on the network
        # this will send a probe to the network and wait for responses
        # See MDPWS discovery mechanisms for details
        print('searching for sdc providers')
        services = my_discovery.search_services(types=SdcV1Definitions.MedicalDeviceTypesFilter)
        # now iterate through the discovered services to check if we foundDevice
        # the specific provider we search for
        for one_service in services:
            print("Got service: {}".format(one_service.epr))
            # the EndPointReference is created based on the UUID of the Provider
            if one_service.epr == device_A_UUID.urn:
                print("Got a match: {}".format(one_service))
                # now create a new SDCClient (=Consumer) that can be used
                # for all interactions with the communication partner
                my_client = SdcConsumer.from_wsd_service(one_service, ssl_context_container=None)
                # start all services on the client to make sure we get updates
                my_client.start_all(not_subscribed_actions=periodic_actions)
                # all data interactions happen through the MDIB (MedicalDeviceInformationBase)
                # that contains data as described in the BICEPS standard
                # this variable will contain the data from the provider
                my_mdib = ConsumerMdib(my_client)
                my_mdib.init_mdib()
                # we can subscribe to updates in the MDIB through the
                # Observable Properties in order to get a callback on
                # specific changes in the MDIB
                observableproperties.bind(my_mdib, metrics_by_handle=on_metric_update)
                # in order to end the 'scan' loop
                found_device = True

                # now we demonstrate how to call a remote operation on the consumer
                set_ensemble_context(my_mdib, my_client)


    # endless loop to keep the client running and get notified on metric changes through callback
    while True:
        time.sleep(1)
