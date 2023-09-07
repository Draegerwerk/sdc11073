import os
import time
import traceback
from collections import defaultdict
from concurrent import futures
from decimal import Decimal

from sdc11073 import commlog
from sdc11073 import observableproperties
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.consumer import SdcConsumer
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types.msg_types import InvocationState

ConsumerMdibMethods.DETERMINATIONTIME_WARN_LIMIT = 2.0

adapter_ip = os.getenv('ref_ip') or '127.0.0.1'  # noqa: SIM112
ca_folder = os.getenv('ref_ca')  # noqa: SIM112
ssl_passwd = os.getenv('ref_ssl_passwd') or None  # noqa: SIM112
search_epr = os.getenv('ref_search_epr') or 'abc'  # noqa: SIM112

ENABLE_COMMLOG = True


def run_ref_test():
    results = []
    print(f'using adapter address {adapter_ip}')
    print('Test step 1: discover device which endpoint ends with "{}"'.format(search_epr))
    wsd = WSDiscovery(adapter_ip)
    wsd.start()
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

    print('Test step 2: connect to device...')
    try:
        if ca_folder:
            ssl_context_container = mk_ssl_contexts_from_folder(ca_folder,
                                                                cyphers_file=None,
                                                                private_key='user_private_key_encrypted.pem',
                                                                certificate='user_certificate_root_signed.pem',
                                                                ca_public_key='root_certificate.pem',
                                                                ssl_passwd=ssl_passwd,
                                                                )
        else:
            ssl_context_container = None
        client = SdcConsumer.from_wsd_service(my_service,
                                              ssl_context_container=ssl_context_container,
                                              validate=True)
        client.start_all()
        print('Test step 2 successful: connected to device')
        results.append('### Test 2 ### passed')
    except:
        print(traceback.format_exc())
        results.append('### Test 2 ### failed')
        return results

    print('Test step 3&4: get mdib and subscribe...')
    try:
        mdib = ConsumerMdib(client)
        mdib.init_mdib()
        print('Test step 3&4 successful')
        results.append('### Test 3 ### passed')
        results.append('### Test 4 ### passed')
    except:
        print(traceback.format_exc())
        results.append('### Test 3 ### failed')
        results.append('### Test 4 ### failed')
        return results

    pm = mdib.data_model.pm_names

    print('Test step 5: check that at least one patient context exists')
    patients = mdib.context_states.NODETYPE.get(pm.PatientContextState, [])
    if len(patients) > 0:
        print('found {} patients, Test step 5 successful'.format(len(patients)))
        results.append('### Test 5 ### passed')
    else:
        print('found no patients, Test step 5 failed')
        results.append('### Test 5 ### failed')

    print('Test step 6: check that at least one location context exists')
    locations = mdib.context_states.NODETYPE.get(pm.LocationContextState, [])
    if len(locations) > 0:
        print('found {} locations, Test step 6 successful'.format(len(locations)))
        results.append('### Test 6 ### passed')
    else:
        print('found no locations, Test step 6 failed')
        results.append('### Test 6 ### failed')

    print('Test step 7&8: count metric state updates and alert state updates')
    metric_updates = defaultdict(list)
    alert_updates = defaultdict(list)

    def onMetricUpdates(metricsbyhandle):
        print('onMetricUpdates', metricsbyhandle)
        for k, v in metricsbyhandle.items():
            metric_updates[k].append(v)

    def onAlertUpdates(alertsbyhandle):
        print('onAlertUpdates', alertsbyhandle)
        for k, v in alertsbyhandle.items():
            alert_updates[k].append(v)

    observableproperties.bind(mdib, metrics_by_handle=onMetricUpdates)
    observableproperties.bind(mdib, alert_by_handle=onAlertUpdates)

    sleep_timer = 20
    min_updates = sleep_timer // 5 - 1
    print('will wait for {} seconds now, expecting at least {} updates per Handle'.format(sleep_timer, min_updates))
    time.sleep(sleep_timer)
    print(metric_updates)
    print(alert_updates)
    if len(metric_updates) == 0:
        results.append('### Test 7 ### failed')
    else:
        for k, v in metric_updates.items():
            if len(v) < min_updates:
                print('found only {} updates for {}, test step 7 failed'.format(len(v), k))
                results.append(f'### Test 7 Handle {k} ### failed')
            else:
                print('found {} updates for {}, test step 7 ok'.format(len(v), k))
                results.append(f'### Test 7 Handle {k} ### passed')
    if len(alert_updates) == 0:
        results.append('### Test 8 ### failed')
    else:
        for k, v in alert_updates.items():
            if len(v) < min_updates:
                print('found only {} updates for {}, test step 8 failed'.format(len(v), k))
                results.append(f'### Test 8 Handle {k} ### failed')
            else:
                print('found {} updates for {}, test step 8 ok'.format(len(v), k))
                results.append(f'### Test 8 Handle {k} ### passed')

    print('Test step 9: call SetString operation')
    setstring_operations = mdib.descriptions.NODETYPE.get(pm.SetStringOperationDescriptor, [])
    setst_handle = 'string.ch0.vmd1_sco_0'
    if len(setstring_operations) == 0:
        print('Test step 9(SetString) failed, no SetString operation found')
        results.append('### Test 9 ### failed')
    else:
        for s in setstring_operations:
            if s.Handle != setst_handle:
                continue
            print('setString Op ={}'.format(s))
            try:
                fut = client.set_service_client.set_string(s.Handle, 'hoppeldipop')
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.InvocationInfo.InvocationState != InvocationState.FINISHED:
                        print('set string operation {} did not finish with "Fin":{}'.format(s.Handle, res))
                        results.append('### Test 9(SetString) ### failed')
                    else:
                        print('set string operation {} ok:{}'.format(s.Handle, res))
                        results.append('### Test 9(SetString) ### passed')
                except futures.TimeoutError:
                    print('timeout error')
                    results.append('### Test 9(SetString) ### failed')
            except Exception as ex:
                print(f'Test 9(SetString): {ex}')
                results.append('### Test 9(SetString) ### failed')

    print('Test step 9: call SetValue operation')
    setvalue_operations = mdib.descriptions.NODETYPE.get(pm.SetValueOperationDescriptor, [])
    #    print('setvalue_operations', setvalue_operations)
    setval_handle = 'numeric.ch0.vmd1_sco_0'
    if len(setvalue_operations) == 0:
        print('Test step 9 failed, no SetValue operation found')
        results.append('### Test 9(SetValue) ### failed')
    else:
        for s in setvalue_operations:
            if s.Handle != setval_handle:
                continue
            print('setNumericValue Op ={}'.format(s))
            try:
                fut = client.set_service_client.set_numeric_value(s.Handle, Decimal('42'))
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.InvocationInfo.InvocationState != InvocationState.FINISHED:
                        print('set value operation {} did not finish with "Fin":{}'.format(s.Handle, res))
                    else:
                        print('set value operation {} ok:{}'.format(s.Handle, res))
                        results.append('### Test 9(SetValue) ### passed')
                except futures.TimeoutError:
                    print('timeout error')
                    results.append('### Test 9(SetValue) ### failed')
            except Exception as ex:
                print(f'Test 9(SetValue): {ex}')
                results.append('### Test 9(SetValue) ### failed')

    print('Test step 9: call Activate operation')
    activate_operations = mdib.descriptions.NODETYPE.get(pm.ActivateOperationDescriptor, [])
    activate_handle = 'actop.vmd1_sco_0'
    if len(setstring_operations) == 0:
        print('Test step 9 failed, no Activate operation found')
        results.append('### Test 9(Activate) ### failed')
    else:
        for s in activate_operations:
            if s.Handle != activate_handle:
                continue
            print('activate Op ={}'.format(s))
            try:
                fut = client.set_service_client.activate(s.Handle, 'hoppeldipop')
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.InvocationInfo.InvocationState != InvocationState.FINISHED:
                        print('activate operation {} did not finish with "Fin":{}'.format(s.Handle, res))
                        results.append('### Test 9(Activate) ### failed')
                    else:
                        print('activate operation {} ok:{}'.format(s.Handle, res))
                        results.append('### Test 9(Activate) ### passed')
                except futures.TimeoutError:
                    print('timeout error')
                    results.append('### Test 9(Activate) ### failed')
            except Exception as ex:
                print(f'Test 9(Activate): {ex}')
                results.append('### Test 9(Activate) ### failed')

    print('Test step 10: cancel all subscriptions')
    success = client._subscription_mgr.unsubscribe_all()
    if success:
        results.append('### Test 10(unsubscribe) ### passed')
    else:
        results.append('### Test 10(unsubscribe) ### failed')
    time.sleep(2)
    return results


if __name__ == '__main__':
    xtra_log_config = os.getenv('ref_xtra_log_cnf')  # noqa: SIM112

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
    comm_logger = commlog.DirectoryLogger(log_folder=r'c:\temp\sdc_refclient_commlog',
                                          log_out=True,
                                          log_in=True,
                                          broadcast_ip_filter=None)
    if ENABLE_COMMLOG:
        for name in commlog.LOGGER_NAMES:
            logging.getLogger(name).setLevel(logging.DEBUG)
        comm_logger.start()
    run_results = run_ref_test()
    for r in run_results:
        print(r)
