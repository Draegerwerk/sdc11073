import os
import time
import traceback
import uuid
from collections import defaultdict
from decimal import Decimal
from sdc11073 import commlog
from sdc11073 import observableproperties
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods
from sdc11073.consumer import SdcConsumer
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_qnames, msg_types

ConsumerMdibMethods.DETERMINATIONTIME_WARN_LIMIT = 2.0

adapter_ip = os.getenv('ref_ip') or '127.0.0.1'
ca_folder = os.getenv('ref_ca')
ssl_passwd = os.getenv('ref_ssl_passwd') or None
search_epr = os.getenv('ref_search_epr') or 'bcd'  # 'bcd' is fixed ending in reference_device v2 uuid.

numeric_metric_handle = "numeric_metric_0.channel_0.vmd_0.mds_0"
alert_condition_handle = "alert_condition_0.vmd_0.mds_1"
set_value_handle = "set_value_0.sco.mds_0"
set_string_handle = "set_string_0.sco.mds_0"
set_context_state_handle = "set_context_0.sco.mds_0"

ENABLE_COMMLOG = False
if ENABLE_COMMLOG:
    comm_logger = commlog.CommLogger(log_folder=r'c:\temp\sdc_refclient_commlog',
                                     log_out=True,
                                     log_in=True,
                                     broadcast_ip_filter=None)
    commlog.set_communication_logger(comm_logger)


sleep_timer = 30


def test_1b(wsd, my_service) -> str:
    # send resolve and check response
    wsd.clear_remote_services()
    wsd._send_resolve(my_service.epr)
    time.sleep(3)
    if len(wsd._remote_services) == 0:
        return ('### Test 1b ### failed, no response')
    elif len(wsd._remote_services) > 1:
        return ('### Test 1b ### failed, multiple response')
    else:
        service = wsd._remote_services.get(my_service.epr)
        if service.epr != my_service.epr:
            return ('### Test 1b ### failed, not the same epr')
        else:
            return ('### Test 1b ### passed')


def connect_client(my_service) -> SdcConsumer:
    if ca_folder:
        ssl_contexts = mk_ssl_contexts_from_folder(ca_folder,
                                                 cyphers_file=None,
                                                 private_key='user_private_key_encrypted.pem',
                                                 certificate='user_certificate_root_signed.pem',
                                                 ca_public_key='root_certificate.pem',
                                                 ssl_passwd=ssl_passwd
                                                 )
    else:
        ssl_contexts = None
    client = SdcConsumer.from_wsd_service(my_service,
                                        ssl_context_container=ssl_contexts,
                                        validate=True)
    client.start_all()
    return client


def init_mdib(client) -> ConsumerMdib:
    # The Reference Provider answers to GetMdib
    mdib = ConsumerMdib(client)
    mdib.init_mdib()
    return mdib


def test_min_updates_per_handle(updates_dict, min_updates, node_type_filter = None) -> (bool, str):  # True ok
    results = []
    is_ok = True
    if len(updates_dict) == 0:
        is_ok = False
        results.append('no updates')
    else:
        for k, v in updates_dict.items():
            if node_type_filter:
                v = [n for n in v if n.NODETYPE == node_type_filter]
            if len(v) < min_updates:
                is_ok = False
                results.append(f'Handle {k} only {len(v)} updates, expect >= {min_updates}')
    return is_ok, '\n'.join(results)


def test_min_updates_for_type(updates_dict, min_updates, q_name) -> (bool, str):  # True ok
    flat_list = []
    for v in updates_dict.values():
        flat_list.extend(v)
    matches = [x for x in flat_list if x.NODETYPE == q_name]
    if len(matches) >= min_updates:
        return True, ''
    return False, f'expect >= {min_updates}, got {len(matches)} out of {len(flat_list)}'


def log_result(is_ok, result_list, step, info, extra_info=None):
    xtra = f' ({extra_info}) ' if extra_info else ''
    if is_ok:
        result_list.append(f'{step} => passed {xtra}{info}')
    else:
        result_list.append(f'{step} => failed {xtra}{info}')


def run_ref_test():
    results = []
    print(f'using adapter address {adapter_ip}')
    print('Test step 1: discover device which endpoint ends with "{}"'.format(search_epr))
    wsd = WSDiscovery(adapter_ip)
    wsd.start()

    # 1. Device Discovery
    # a) The Reference Provider sends Hello messages
    # b) The Reference Provider answers to Probe and Resolve messages

    my_service = None
    while my_service is None:
        services = wsd.search_services(types=SdcV1Definitions.MedicalDeviceTypesFilter)
        print('found {} services {}'.format(len(services), ', '.join([s.epr for s in services])))
        for s in services:
            if s.epr.endswith(search_epr):
                my_service = s
                print('found service {}'.format(s.epr))
                break
    print('Test step 1 successful: device discovered')
    results.append('### Test 1 ### passed')

    print('Test step 1b: send resolve and check response')
    result = test_1b(wsd, my_service)
    results.append(result)
    print(f'{result} : resolve and check response')

    # 2. BICEPS Services Discovery and binding
    # a) The Reference Provider answers to TransferGet
    # b) The SDCri Reference Provider grants subscription runtime of at most 15 seconds in order to enforce Reference Consumers to send renew requests

    """2. BICEPS Services Discovery and binding
        a) The Reference Provider answers to TransferGet
        b) The Reference Consumer renews at least one subscription once during the test phase; 
           the Reference Provider grants subscriptions of at most 15 seconds 
           (this allows for the Reference Consumer to verify if auto-renew works)"""
    step = '2a'
    info = 'The Reference Provider answers to TransferGet'
    print(step, info)
    try:
        client = connect_client(my_service)
        log_result(client.host_description is not None, results, step, info)
    except:
        print(traceback.format_exc())
        results.append(f'{step} => failed')
        return results

    step = '2b.1'
    info = 'the Reference Provider grants subscriptions of at most 15 seconds'
    now = time.time()
    durations = [s.expires_at - now for s in client.subscription_mgr.subscriptions.values()]
    print(f'subscription durations = {durations}')
    log_result(max(durations) <= 15, results, step, info)
    step = '2b.2'
    info = 'the Reference Provider grants subscriptions of at most 15 seconds (renew)'
    granted = list(client.subscription_mgr.subscriptions.items())[0][1].renew(30000)
    print(f'renew granted = {granted}')
    log_result(max(durations) <= 15, results, step, info)

    # 3. Request Response
    # a) The Reference Provider answers to GetMdib
    # b) The Reference Provider answers to GetContextStates messages
    # b.1) The Reference Provider provides at least one location context state
    step = '3a'
    info = 'The Reference Provider answers to GetMdib'
    print(step, info)
    try:
        mdib = init_mdib(client)
        log_result(mdib is not None, results, step, info)
    except:
        print(traceback.format_exc())
        results.append(f'{step} => failed')
        return results

    step = '3b'
    info = 'The Reference Provider answers to GetContextStates messages'
    context_service = client.context_service_client
    if context_service is None:
        results.append(f'{step} => failed {info}')
    else:
        try:
            states = context_service.get_context_states().result.ContextState
            results.append(f'{step} => passed {info}')
        except:
            print(traceback.format_exc())
            results.append(f'{step} => failed {info}')
            return results
        step = 'Test step 3b.1: The Reference Provider provides at least one location context state'
        loc_states = [ s for s in states if s.NODETYPE == pm_qnames.LocationContextState]
        log_result(len(loc_states) > 0, results, step, info)

    # 4 State Reports
    # a)The Reference Provider produces at least 5 metric updates in 30 seconds
    #   The metric types shall comprise numeric and string metrics
    # b) The Reference Provider produces at least 5 alert condition updates in 30 seconds
    # c) The Reference Provider produces at least 5 alert signal updates in 30 seconds
    # d) The Reference Provider provides alert system self checks in accordance to the periodicity defined in the MDIB (at least every 10 seconds)
    # e) The Reference Provider provides 3 waveforms x 10 messages per second x 100 samples per message
    # f) The Reference Provider provides changes for the following components:
    #   * Clock/Battery object (Component report)
    #   * The Reference Provider provides changes for the VMD/MDS (Component report)
    # g) The Reference Provider provides changes for the following operational states:
    #    Enable/Disable operations (some different than the ones mentioned above) (Operational State Report)"""

    # setup data collectors for next test steps
    numeric_metric_updates = defaultdict(list)
    string_metric_updates = defaultdict(list)
    alert_condition_updates = defaultdict(list)
    alert_signal_updates = defaultdict(list)
    # other_alert_updates = defaultdict(list)
    alert_system_updates = defaultdict(list)
    component_updates = defaultdict(list)
    waveform_updates = defaultdict(list)
    description_updates = []

    def onMetricUpdates(metrics_by_handle):
        # print('onMetricUpdates', metrics_by_handle)
        for k, v in metrics_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            if v.NODETYPE == pm_qnames.NumericMetricState:
                numeric_metric_updates[k].append(v)
            elif v.NODETYPE == pm_qnames.StringMetricState:
                string_metric_updates[k].append(v)

    def on_alert_updates(alerts_by_handle):
        for k, v in alerts_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            if v.is_alert_condition:
                alert_condition_updates[k].append(v)
            elif v.is_alert_signal:
                alert_signal_updates[k].append(v)
            elif v.NODETYPE == pm_qnames.AlertSystemState:
                alert_system_updates[k].append(v)
            # else:
            #     other_alert_updates[k].append(v)

    def on_component_updates(components_by_handle):
        # print('on_component_updates', alerts_by_handle)
        for k, v in components_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            component_updates[k].append(v)

    def on_waveform_updates(waveforms_by_handle):
        for k, v in waveforms_by_handle.items():
            waveform_updates[k].append(v)

    def on_description_modification(description_modification_report):
        print('on_description_modification')
        description_updates.append(description_modification_report)

    observableproperties.bind(mdib, metrics_by_handle=onMetricUpdates)
    observableproperties.bind(mdib, alert_by_handle=on_alert_updates)
    observableproperties.bind(mdib, component_by_handle=on_component_updates)
    observableproperties.bind(mdib, waveform_by_handle=on_waveform_updates)
    observableproperties.bind(mdib, description_modifications=on_description_modification)

    min_updates = sleep_timer // 5 - 1
    print('will wait for {} seconds now, expecting at least {} updates per Handle'.format(sleep_timer, min_updates))
    time.sleep(sleep_timer)

    step = '4a'
    info ='count numeric metric state updates'
    print(step, info)
    is_ok, result = test_min_updates_per_handle(numeric_metric_updates, min_updates)
    log_result(is_ok, results, step, info)

    step = '4b'
    info = 'count string metric state updates'
    print(step)
    is_ok, result = test_min_updates_per_handle(string_metric_updates, min_updates,)
    log_result(is_ok, results, step, info)

    step = '4c'
    info = 'count alert condition updates'
    print(step)
    is_ok, result = test_min_updates_per_handle(alert_condition_updates, min_updates)
    log_result(is_ok, results, step, info)

    step = '4d'
    info =' count alert signal updates'
    print(step, info)
    is_ok, result = test_min_updates_per_handle(alert_signal_updates, min_updates)
    log_result(is_ok, results, step, info)

    step ='4e'
    info = 'count alert system self checks'
    is_ok, result = test_min_updates_per_handle(alert_system_updates, min_updates)
    log_result(is_ok, results, step, info)

    step = '4f'
    info = 'count waveform updates'
    print(step, info)
    is_ok, result = test_min_updates_per_handle(waveform_updates, min_updates)
    log_result(is_ok, results, step, info+ ' notifications per second')
    if len(waveform_updates) < 3:
        log_result(False, results, step, info+' number of waveforms')
    else:
        log_result(True, results, step, info+' number of waveforms')

    expected_samples = 1000 * sleep_timer*0.9
    for handle, reports in waveform_updates.items():
        notifications = [n for n in reports if n.MetricValue is not None]
        samples = sum([len(n.MetricValue.Samples) for n in notifications])
        if samples < expected_samples:
            log_result(False, results, step, info + f' waveform {handle} has {samples} samples, expecting {expected_samples}')
            is_ok = False
        else:
            log_result(True, results, step, info + f' waveform {handle} has {samples} samples')

    pm = mdib.data_model.pm_names
    pm_types = mdib.data_model.pm_types

    # The Reference Provider provides changes for the following reports as well:
    # Clock/Battery object (Component report)
    step = '4g'
    info = 'count battery updates'
    print(step, info)
    is_ok, result = test_min_updates_for_type(component_updates, 1, pm.BatteryState)
    log_result(is_ok, results, step, info)

    step = '4g'
    info ='count VMD updates'
    print(step, info)
    is_ok, result = test_min_updates_for_type(component_updates, 1, pm.VmdState)
    log_result(is_ok, results, step, info)

    step = '4g'
    info = 'count MDS updates'
    print(step, info)
    is_ok, result = test_min_updates_for_type(component_updates, 1, pm.MdsState)
    log_result(is_ok, results, step, info)

    step = '4h'
    info = 'Enable/Disable operations'
    results.append(f'{step} => failed, not implemented {info}')

    """
    5 Description Modifications:
    a) The Reference Provider produces at least 1 update every 10 seconds comprising
        * Update Alert condition concept description of type
        * Update Alert condition cause-remedy information
        * Update Unit of measure (metrics)
    b)  The Reference Provider produces at least 1 insertion followed by a deletion every 10 seconds comprising
        * Insert a VMD including Channels including metrics
        * Remove the VMD
    """
    step = '5a'
    info = 'Update Alert condition concept description of type'
    print(step, info)
    # verify only that there are Alert Condition Descriptors updated
    found = False
    for report in description_updates:
        for report_part in report.ReportPart:
            if report_part.ModificationType == msg_types.DescriptionModificationType.UPDATE:
                for descriptor in report_part.Descriptor:
                    if descriptor.NODETYPE == pm_qnames.AlertConditionDescriptor:
                        found = True
    log_result(found, results, step, info)

    step = '5a'
    info = 'Update Unit of measure'
    print(step, info)
    # verify only that there are Alert Condition Descriptors updated
    found = False
    for report in description_updates:
        for report_part in report.ReportPart:
            if report_part.ModificationType == msg_types.DescriptionModificationType.UPDATE:
                for descriptor in report_part.Descriptor:
                    if descriptor.NODETYPE == pm_qnames.NumericMetricDescriptor:
                        found = True
    log_result(found, results, step, info)

    step = '5b'
    info = 'Add / remove vmd'
    print(step, info)
    # verify only that there are Alert Condition Descriptors updated
    add_found = False
    rm_found = False
    for report in description_updates:
        for report_part in report.ReportPart:
            if report_part.ModificationType == msg_types.DescriptionModificationType.CREATE:
                for descriptor in report_part.Descriptor:
                    if descriptor.NODETYPE == pm_qnames.VmdDescriptor:
                        add_found = True
            if report_part.ModificationType == msg_types.DescriptionModificationType.DELETE:
                for descriptor in report_part.Descriptor:
                    if descriptor.NODETYPE == pm_qnames.VmdDescriptor:
                        rm_found = True
    log_result(add_found, results, step, info, 'add')
    log_result(rm_found, results, step, info, 'remove')

    """
    6 Operation invocation
    a) (removed)
    b) SetContextState:
        * Payload: 1 Patient Context 
        * Context state is added to the MDIB including context association and validation
        * If there is an associated context already, that context shall be disassociated
            * Handle and version information is generated by the provider
        * In order to avoid infinite growth of patient contexts, older contexts are allowed to be removed from the MDIB (=ContextAssociation=No)
    c) SetValue: Immediately answers with "finished"
        * Finished has to be sent as a report in addition to the response => 
    d) SetString: Initiates a transaction that sends Wait, Start and Finished
    e) SetMetricStates:
        * Payload: 2 Metric States (settings; consider alert limits)
        * Immediately sends finished
        * Action: Alter values of metrics """

    step = '6b'
    info = 'SetContextState'
    print(step, info)
    # patients = mdib.context_states.NODETYPE.get(pm.PatientContextState, [])
    patient_context_descriptors = mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor, [])
    generated_family_names = []
    if len(patient_context_descriptors) == 0:
        log_result(False, results, step, info, extra_info='no PatientContextDescriptor')
    else:
        try:
            for i, p in enumerate(patient_context_descriptors):
                pat = client.context_service_client.mk_proposed_context_object(p.Handle)
                pat.CoreData.Familyname = uuid.uuid4().hex
                pat.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
                generated_family_names.append(pat.CoreData.Familyname)
                client.context_service_client.set_context_state(set_context_state_handle, [pat])
            time.sleep(1)  # allow update notification to arrive
            patients = mdib.context_states.NODETYPE.get(pm_qnames.PatientContextState, [])
            if len(patients) == 0:
                log_result(False, results, step, info, extra_info='no patients found')
            else:
                all_ok = True
                for patient in patients:
                    if patient.CoreData.Familyname in generated_family_names:
                        if patient.ContextAssociation != pm_types.ContextAssociation.ASSOCIATED:
                            log_result(False, results, step, info,
                                       extra_info=f'new patient {patient.CoreData.Familyname} is {patient.ContextAssociation}')
                            all_ok = False
                    else:
                        if patient.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED:
                            log_result(False, results, step, info,
                                       extra_info=f'old patient {patient.CoreData.Familyname} is {patient.ContextAssociation}')
                            all_ok = False
                log_result(all_ok, results, step, info)
        except Exception as ex:
            print(traceback.format_exc())
            log_result(False, results, step, info, ex)

    step = '6c'
    info = 'SetValue: Immediately answers with "finished"'
    print(step, info)
    subscriptions = client.subscription_mgr.subscriptions.values()
    operation_invoked_subscriptions = [subscr for subscr in subscriptions
                                       if 'OperationInvokedReport' in subscr.short_filter_string]
    if len(operation_invoked_subscriptions) == 0:
        log_result(False, results, step, info, 'OperationInvokedReport not subscribed, cannot test')
    elif len(operation_invoked_subscriptions) > 1:
        log_result(False, results, step, info,
                   f'found {len(operation_invoked_subscriptions)} OperationInvokedReport subscribed, cannot test')
    else:
        try:
            operations = client.mdib.descriptions.NODETYPE.get(pm_qnames.SetValueOperationDescriptor, [])
            my_ops = [op for op in operations if op.Type.Code == "67108888"]
            if len(my_ops) != 1:
                log_result(False, results, step, info, f'found {len(my_ops)} operations with code "67108888"')
            else:
                operation = my_ops[0]
                future_object = client.set_service_client.set_numeric_value(operation.Handle, Decimal(42))
                operation_result = future_object.result()
                if len(operation_result.report_parts) == 0:
                    log_result(False, results, step, info, 'no notification')
                elif len(operation_result.report_parts) > 1:
                    log_result(False, results, step, info, f'got {len(operation_result.report_parts)} notifications, expect only one')
                else:
                    log_result(True, results, step, info, f'got {len(operation_result.report_parts)} notifications')
                if operation_result.InvocationInfo.InvocationState != msg_types.InvocationState.FINISHED:
                    log_result(False, results, step, info,
                               f'got result {operation_result.InvocationInfo.InvocationState} '
                               f'{operation_result.InvocationInfo.InvocationError} '
                               f'{operation_result.InvocationInfo.InvocationErrorMessage}')
        except Exception as ex:
            print(traceback.format_exc())
            log_result(False, results, step, info, ex)

    step = '6d'
    info = 'SetString: Initiates a transaction that sends Wait, Start and Finished'
    print(step, info)
    try:
        operations = client.mdib.descriptions.NODETYPE.get(pm_qnames.SetStringOperationDescriptor, [])
        my_ops = [op for op in operations if op.Type.Code == "67108889"]
        if len(my_ops) != 1:
            log_result(False, results, step, info, f'found {len(my_ops)} operations with code "67108889"')
        else:
            operation = my_ops[0]
            future_object = client.set_service_client.set_string(operation.Handle, 'STANDBY')
            operation_result = future_object.result()
            if len(operation_result.report_parts) == 0:
                log_result(False, results, step, info, 'no notification')
            elif len(operation_result.report_parts) >= 3:
                # check order of operation invoked reports (simple expectation, there could be multiple WAIT in theory)
                expectation = [msg_types.InvocationState.WAIT,
                                  msg_types.InvocationState.START,
                                  msg_types.InvocationState.FINISHED]
                inv_states = [p.InvocationInfo.InvocationState for p in operation_result.report_parts]
                if inv_states != expectation:
                    log_result(False, results, step, info, f'wrong order {inv_states}')
                else:
                    log_result(True, results, step, info, f'got {len(operation_result.report_parts)} notifications')
            if operation_result.InvocationInfo.InvocationState != msg_types.InvocationState.FINISHED:
                log_result(False, results, step, info,
                           f'got result {operation_result.InvocationInfo.InvocationState} '
                           f'{operation_result.InvocationInfo.InvocationError} '
                           f'{operation_result.InvocationInfo.InvocationErrorMessage}')

    except Exception as ex:
        print(traceback.format_exc())
        log_result(False, results, step, info, ex)

    step = '6e'
    info = 'SetMetricStates Immediately answers with finished'
    print(step, info)
    try:
        operations = client.mdib.descriptions.NODETYPE.get(pm_qnames.SetMetricStateOperationDescriptor, [])
        my_ops = [op for op in operations if op.Type.Code == "67108890"]
        if len(my_ops) != 1:
            log_result(False, results, step, info, f'found {len(my_ops)} operations with code "67108890"')
        else:
            operation = my_ops[0]
            proposed_metric_state1 = client.mdib.xtra.mk_proposed_state("numeric_metric_0.channel_0.vmd_1.mds_0")
            proposed_metric_state2 = client.mdib.xtra.mk_proposed_state("numeric_metric_1.channel_0.vmd_1.mds_0")
            for st in (proposed_metric_state1, proposed_metric_state2):
                if st.MetricValue is None:
                    st.mk_metric_value()
                    st.MetricValue.Value = Decimal(1)
                else:
                    st.MetricValue.Value += Decimal(0.1)
            future_object = client.set_service_client.set_metric_state(operation.Handle, [proposed_metric_state1, proposed_metric_state2])
            operation_result = future_object.result()
            if len(operation_result.report_parts) == 0:
                log_result(False, results, step, info, 'no notification')
            elif len(operation_result.report_parts) > 1:
                log_result(False, results, step, info, f'got {len(operation_result.report_parts)} notifications, expect only one')
            else:
                log_result(True, results, step, info, f'got {len(operation_result.report_parts)} notifications')
            if operation_result.InvocationInfo.InvocationState != msg_types.InvocationState.FINISHED:
                log_result(False, results, step, info,
                           f'got result {operation_result.InvocationInfo.InvocationState} '
                           f'{operation_result.InvocationInfo.InvocationError} '
                           f'{operation_result.InvocationInfo.InvocationErrorMessage}')
    except Exception as ex:
        print(traceback.format_exc())
        log_result(False, results, step, info, ex)

    step = '7'
    info = 'Graceful shutdown (at least subscriptions are ended; optionally Bye is sent)'
    try:
        success = client._subscription_mgr.unsubscribe_all()
        log_result(success, results, step, info)
    except Exception as ex:
        print(traceback.format_exc())
        log_result(False, results, step, info, ex)
    time.sleep(2)
    return results


if __name__ == '__main__':
    xtra_log_config = os.getenv('ref_xtra_log_cnf')  # or None

    import json
    import logging.config

    here = os.path.dirname(__file__)

    with open(os.path.join(here, 'logging_default.json')) as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    if xtra_log_config is not None:
        with open(xtra_log_config) as f:
            logging_setup2 = json.load(f)
            logging.config.dictConfig(logging_setup2)

    run_results = run_ref_test()
    print('\n### Summary ###')
    for r in run_results:
        print(r)
