from __future__ import annotations

import time
import uuid
import logging
from decimal import Decimal

from sdc11073.location import SdcLocation
from sdc11073.mdib import ProviderMdib
from sdc11073.provider import SdcProvider
from sdc11073.provider.components import SdcProviderComponents
from sdc11073.roles.product import ExtendedProduct
from sdc11073.wsdiscovery import WSDiscoverySingleAdapter
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.loghelper import basic_logging_setup

# example SDC provider (device) that sends out metrics every now and then


# The provider we use, should match the one in consumer example
# The UUID is created from a base
base_uuid = uuid.UUID('{cc013678-79f6-403c-998f-3cc0cc050230}')
my_uuid = uuid.uuid5(base_uuid, "12345")


# setting the local ensemble context upfront
def set_local_ensemble_context(mdib: ProviderMdib, ensemble_extension_string: str):
    descriptor_container = mdib.descriptions.NODETYPE.get_one(pm.EnsembleContextDescriptor)
    if not descriptor_container:
        print("No ensemble contexts in mdib")
        return
    all_ensemble_context_states = mdib.context_states.descriptor_handle.get(descriptor_container.Handle, [])
    with mdib.transaction_manager() as mgr:
        # set all to currently associated Locations to Disassociated
        associated_ensemble_context_states = [l for l in all_ensemble_context_states if
                                              l.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
        for tmp in associated_ensemble_context_states:
            ensemble_context_state = mgr.get_context_state(tmp.DescriptorHandle, tmp.Handle)
            ensemble_context_state.ContextAssociation = pm_types.ContextAssociation.DISASSOCIATED
            # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
            ensemble_context_state.UnbindingMdibVersion = mdib.mdib_version

        new_ensemble_context_state = mgr.mk_context_state(descriptor_container.Handle, set_associated=True)
        new_ensemble_context_state.Identification = [
            pm_types.InstanceIdentifier(root="1.2.3", extension_string=ensemble_extension_string)]


if __name__ == '__main__':
    # start with discovery (MDPWS) that is running on the named adapter "Ethernet" (replace as you need it on your machine, e.g. "enet0" or "Ethernet")
    basic_logging_setup(level=logging.INFO)

    my_discovery = WSDiscoverySingleAdapter("Loopback Pseudo-Interface 1")
    # start the discovery
    my_discovery.start()
    # create a local mdib that will be sent out on the network, the mdib is based on a XML file
    my_mdib = ProviderMdib.from_mdib_file("mdib.xml")
    print("My UUID is {}".format(my_uuid))
    # set a location context to allow easy discovery
    my_location = SdcLocation(fac='HOSP', poc='CU2', bed='BedSim')
    # set model information for discovery
    dpws_model = ThisModelType(manufacturer='Draeger',
                              manufacturer_url='www.draeger.com',
                              model_name='TestDevice',
                              model_number='1.0',
                              model_url='www.draeger.com/model',
                              presentation_url='www.draeger.com/model/presentation')
    dpws_device = ThisDeviceType(friendly_name='TestDevice',
                                firmware_version='Version1',
                                serial_number='12345')
    # create a device (provider) class that will do all the SDC magic
    # set role provider that supports Ensemble Contexts.
    specific_components = SdcProviderComponents(sco_role_provider_class=ExtendedProduct)
    sdc_provider = SdcProvider(ws_discovery=my_discovery,
                               epr=my_uuid,
                               this_model=dpws_model,
                               this_device=dpws_device,
                               device_mdib_container=my_mdib,
                               specific_components=specific_components
                               )
    # start the local device and make it discoverable
    sdc_provider.start_all()
    # set the local ensemble context to ease discovery based on ensemble ID
    set_local_ensemble_context(my_mdib, "MyEnsemble")
    # set the location on our device
    sdc_provider.set_location(my_location)
    # create one local numeric metric that will change later on
    # get all metrics from the mdib (as described in the file)
    all_metric_descrs = [c for c in my_mdib.descriptions.objects if c.NODETYPE == pm.NumericMetricDescriptor]
    # now change all the metrics in one transaction
    with my_mdib.transaction_manager() as mgr:
        for metric_descr in all_metric_descrs:
            # get the metric state of this specific metric
            st = mgr.get_state(metric_descr.Handle)
            # create a value in case it is not there yet
            st.mk_metric_value()
            # set the value and some other fields to a fixed value
            st.MetricValue.Value = Decimal(1.0)
            st.MetricValue.ActiveDeterminationPeriod = 1494554822450
            st.MetricValue.Validity = pm_types.MeasurementValidity.VALID
            st.ActivationState = pm_types.ComponentActivation.ON

    # now iterate forever and change the value every few seconds
    metric_value = 0
    while True:
        metric_value += 1
        with my_mdib.transaction_manager() as mgr:
            for metricDescr in all_metric_descrs:
                st = mgr.get_state(metricDescr.Handle)
                st.MetricValue.Value = Decimal(metric_value)
        time.sleep(5)
