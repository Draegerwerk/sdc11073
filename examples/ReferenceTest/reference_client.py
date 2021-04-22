import time
import logging
import traceback
import os
import sdc11073
from collections import defaultdict
from sdc11073 import observableproperties
from sdc11073.definitions_sdc import SDC_v1_Definitions
from concurrent import futures
from sdc11073.certloader import mk_ssl_context_from_folder


adapter_ip = os.getenv('ref_ip') or '127.0.0.1'
ca_folder = os.getenv('ref_ca')
ssl_passwd = os.getenv('ref_ssl_passwd') or None
search_epr = os.getenv('ref_search_epr')  or 'abc' # abc is fixed ending in reference_device uuid.


def run_ref_test():
    results = []
    print('Test step 1: discover device which endpoint ends with "{}"'.format(search_epr))
    wsd = sdc11073.wsdiscovery.WSDiscoveryWhitelist([adapter_ip])
    wsd.start()
    my_service = None
    while my_service is None:
        services = wsd.searchServices(types=SDC_v1_Definitions.MedicalDeviceTypesFilter)
        print('found {} services {}'.format(len(services), ', '.join([s.getEPR() for s in services])))
        for s in services:
            if s.getEPR().endswith(search_epr):
                my_service = s
                print('found service {}'.format(s.getEPR()))
                break
    print('Test step 1 successful: device discovered')
    results.append('### Test 1 ### passed')

    print('Test step 2: connect to device...')
    try:
        if ca_folder:
            ssl_context = mk_ssl_context_from_folder(ca_folder, cyphers_file=None,
                                                     certificate='sdccert.pem',
                                                     ssl_passwd=ssl_passwd,
                                                     )
        else:
            ssl_context = None
        client = sdc11073.sdcclient.SdcClient.fromWsdService(my_service,
                                                             sslContext=ssl_context)
        client.startAll()
        print('Test step 2 successful: connected to device')
        results.append('### Test 2 ### passed')
    except:
        print (traceback.format_exc())
        results.append('### Test 2 ### failed')
        return results

    print('Test step 3&4: get mdib and subscribe...')
    try:
        mdib = sdc11073.mdib.clientmdib.ClientMdibContainer(client)
        mdib.initMdib()
        print('Test step 3&4 successful')
        results.append('### Test 3 ### passed')
        results.append('### Test 4 ### passed')
    except:
        print(traceback.format_exc())
        results.append('### Test 3 ### failed')
        results.append('### Test 4 ### failed')
        return results

    print('Test step 5: check that at least one patient context exists')
    patients = mdib.contextStates.NODETYPE.get(sdc11073.namespaces.domTag('PatientContextState'), [])
    if len(patients) > 0:
        print('found {} patients, Test step 5 successful'.format(len(patients)))
        results.append('### Test 5 ### passed')
    else:
        print('found no patients, Test step 5 failed')
        results.append('### Test 5 ### failed')

    print('Test step 6: check that at least one location context exists')
    locations = mdib.contextStates.NODETYPE.get(sdc11073.namespaces.domTag('LocationContextState'), [])
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

    observableproperties.bind(mdib, metricsByHandle=onMetricUpdates)
    observableproperties.bind(mdib, alertByHandle=onAlertUpdates)

    sleep_timer = 20
    min_updates = sleep_timer // 5 - 1
    print('will wait for {} seconds now, expecting at least {} updates per handle'.format(sleep_timer, metric_updates))
    time.sleep(sleep_timer)
    print(metric_updates)
    print(alert_updates)
    if len(metric_updates) == 0:
        results.append('### Test 7 ### failed')
    else:
        for k, v in metric_updates.items():
            if len(v) < min_updates:
                print('found only {} updates for {}, test step 7 failed'.format(len(v), k))
                results.append(f'### Test 7 handle {k} ### failed')
            else:
                print('found {} updates for {}, test step 7 ok'.format(len(v), k))
                results.append(f'### Test 7 handle {k} ### passed')
    if len(alert_updates) == 0:
        results.append('### Test 8 ### failed')
    else:
        for k, v in alert_updates.items():
            if len(v) < min_updates:
                print('found only {} updates for {}, test step 8 failed'.format(len(v), k))
                results.append(f'### Test 8 handle {k} ### failed')
            else:
                print('found {} updates for {}, test step 8 ok'.format(len(v), k))
                results.append(f'### Test 8 handle {k} ### passed')

    print('Test step 9: call SetString operation')
    setstring_operations = mdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('SetStringOperationDescriptor'), [])
    setst_handle = 'string.ch0.vmd1_sco_0'
    if len(setstring_operations) == 0:
        print('Test step 9(SetString) failed, no SetString operation found')
        results.append('### Test 9 ### failed')
    else:
        for s in setstring_operations:
            if s.handle != setst_handle:
                continue
            print('setString Op ={}'.format(s))
            fut = client.SetService_client.setString(s.handle, 'hoppeldipop')
            try:
                res = fut.result(timeout=10)
                print(res)
                if res.state != sdc11073.pmtypes.InvocationState.FINISHED:
                    print('set string operation {} did not finish with "Fin":{}'.format(s.handle, res))
                    results.append('### Test 9(SetString) ### failed')
                else:
                    print('set value operation {} ok:{}'.format(s.handle, res))
                    results.append('### Test 9(SetString) ### passed')
            except futures.TimeoutError:
                print('timeout error')
                results.append('### Test 9(SetString) ### failed')

    print('Test step 9: call SetValue operation')
    setvalue_operations = mdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('SetValueOperationDescriptor'), [])
#    print('setvalue_operations', setvalue_operations)
    setval_handle = 'numeric.ch0.vmd1_sco_0'
    if len(setvalue_operations) == 0:
        print('Test step 9 failed, no SetValue operation found')
        results.append('### Test 9(SetValue) ### failed')
    else:
        for s in setvalue_operations:
            if s.handle != setval_handle:
                continue
            print('setNumericValue Op ={}'.format(s))
            fut = client.SetService_client.setNumericValue(s.handle, 42)
            try:
                res = fut.result(timeout=10)
                print(res)
                if res.state != sdc11073.pmtypes.InvocationState.FINISHED:
                    print('set value operation {} did not finish with "Fin":{}'.format(s.handle, res))
                else:
                    print('set value operation {} ok:{}'.format(s.handle, res))
                    results.append('### Test 9(SetValue) ### passed')
            except futures.TimeoutError:
                print('timeout error')
                results.append('### Test 9(SetValue) ### failed')

    print('Test step 9: call Activate operation')
    activate_operations = mdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('ActivateOperationDescriptor'), [])
    activate_handle = 'actop.vmd1_sco_0'
    if len(setstring_operations) == 0:
        print('Test step 9 failed, no Activate operation found')
        results.append('### Test 9(Activate) ### failed')
    else:
        for s in activate_operations:
            if s.handle != activate_handle:
                continue
            print('activate Op ={}'.format(s))
            fut = client.SetService_client.activate(s.handle, 'hoppeldipop')
            try:
                res = fut.result(timeout=10)
                print(res)
                if res.state != sdc11073.pmtypes.InvocationState.FINISHED:
                    print('set string operation {} did not finish with "Fin":{}'.format(s.handle, res))
                    results.append('### Test 9(Activate) ### failed')
                else:
                    print('set value operation {} ok:{}'.format(s.handle, res))
                    results.append('### Test 9(Activate) ### passed')
            except futures.TimeoutError:
                print('timeout error')
                results.append('### Test 9(Activate) ### failed')

    return results



if __name__ == '__main__':

    results = run_ref_test()
    for r in results:
        print(r)
