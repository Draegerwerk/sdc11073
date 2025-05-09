"""Reference test v1."""

from __future__ import annotations

import dataclasses
import enum
import os
import pathlib
import sys
import time
import traceback
import uuid
from collections import defaultdict
from concurrent import futures
from decimal import Decimal

import sdc11073.certloader
from sdc11073 import commlog, network, observableproperties
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.consumer import SdcConsumer
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types.msg_types import InvocationState

ConsumerMdibMethods.DETERMINATIONTIME_WARN_LIMIT = 2.0

# ref_discovery_runs indicates the maximum executions of wsdiscovery search services, "0" -> run until service is found
discovery_runs = int(os.getenv('ref_discovery_runs', '0'))  # noqa: SIM112

ENABLE_COMMLOG = True


def get_network_adapter() -> network.NetworkAdapter:
    """Get network adapter from environment or first loopback."""
    if (ip := os.getenv('ref_ip')) is not None:  # noqa: SIM112
        return network.get_adapter_containing_ip(ip)
    # get next available loopback adapter
    return next(adapter for adapter in network.get_adapters() if adapter.is_loopback)


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


class TestResult(enum.Enum):
    """Represents the overall test result."""

    PASSED = 'PASSED'
    FAILED = 'FAILED'


@dataclasses.dataclass
class TestCollector:
    """Test collector."""

    overall_test_result: TestResult = TestResult.PASSED
    test_messages: list = dataclasses.field(default_factory=list)

    def add_result(self, test_step_message: str, test_step_result: TestResult):
        """Add result to result list."""
        if not isinstance(test_step_result, TestResult):
            raise TypeError('Unexpected parameter')
        if self.overall_test_result is not TestResult.FAILED:
            self.overall_test_result = test_step_result
        self.test_messages.append(test_step_message)


def run_ref_test() -> TestCollector:  # noqa: PLR0915,PLR0912,C901
    """Run reference tests."""
    test_collector = TestCollector()
    adapter_ip = get_network_adapter().ip
    print(f'using adapter address {adapter_ip}')
    search_epr = get_epr()
    print(f'Test step 1: discover device which endpoint ends with "{search_epr}"')
    wsd = WSDiscovery(str(adapter_ip))
    wsd.start()
    my_service = None
    discovery_counter = 0
    while my_service is None:
        services = wsd.search_services(types=SdcV1Definitions.MedicalDeviceTypesFilter)
        print('found {} services {}'.format(len(services), ', '.join([s.epr for s in services])))
        for s in services:
            if s.epr.endswith(str(search_epr)):
                my_service = s
                print(f'found service {s.epr}')
                break
        discovery_counter += 1
        if discovery_runs and discovery_counter >= discovery_runs:
            print('### Test 1 ### failed - No suitable service was discovered')
            test_collector.add_result('### Test 1 ### failed', TestResult.FAILED)
            return test_collector
    print('Test step 1 passed: device discovered')
    test_collector.add_result('### Test 1 ### passed', TestResult.PASSED)

    print('Test step 2: connect to device...')
    try:
        client = SdcConsumer.from_wsd_service(my_service, ssl_context_container=get_ssl_context(), validate=True)
        client.start_all()
        print('Test step 2 passed: connected to device')
        test_collector.add_result('### Test 2 ### passed', TestResult.PASSED)
    except Exception:  # noqa: BLE001
        print(traceback.format_exc())
        test_collector.add_result('### Test 2 ### failed', TestResult.FAILED)
        return test_collector

    print('Test step 3&4: get mdib and subscribe...')
    try:
        mdib = ConsumerMdib(client)
        mdib.init_mdib()
        print('Test step 3&4 passed')
        test_collector.add_result('### Test 3 ### passed', TestResult.PASSED)
        test_collector.add_result('### Test 4 ### passed', TestResult.PASSED)
    except Exception:  # noqa: BLE001
        print(traceback.format_exc())
        test_collector.add_result('### Test 3 ### failed', TestResult.FAILED)
        test_collector.add_result('### Test 4 ### failed', TestResult.FAILED)
        return test_collector

    pm = mdib.data_model.pm_names

    print('Test step 5: check that at least one patient context exists')
    patients = mdib.context_states.NODETYPE.get(pm.PatientContextState, [])
    if len(patients) > 0:
        print(f'found {len(patients)} patients, Test step 5 passed')
        test_collector.add_result('### Test 5 ### passed', TestResult.PASSED)
    else:
        print('found no patients, Test step 5 failed')
        test_collector.add_result('### Test 5 ### failed', TestResult.FAILED)

    print('Test step 6: check that at least one location context exists')
    locations = mdib.context_states.NODETYPE.get(pm.LocationContextState, [])
    if len(locations) > 0:
        print(f'found {len(locations)} locations, Test step 6 passed')
        test_collector.add_result('### Test 6 ### passed', TestResult.PASSED)
    else:
        print('found no locations, Test step 6 failed')
        test_collector.add_result('### Test 6 ### failed', TestResult.FAILED)

    print('Test step 7&8: count metric state updates and alert state updates')
    metric_updates = defaultdict(list)
    alert_updates = defaultdict(list)

    def _on_metric_updates(metricsbyhandle: dict):
        print('onMetricUpdates', metricsbyhandle)
        for k, v in metricsbyhandle.items():
            metric_updates[k].append(v)

    def _on_alert_updates(alertsbyhandle: dict):
        print('onAlertUpdates', alertsbyhandle)
        for k, v in alertsbyhandle.items():
            alert_updates[k].append(v)

    observableproperties.bind(mdib, metrics_by_handle=_on_metric_updates)
    observableproperties.bind(mdib, alert_by_handle=_on_alert_updates)

    sleep_timer = 20
    min_updates = sleep_timer // 5 - 1
    print(f'will wait for {sleep_timer} seconds now, expecting at least {min_updates} updates per Handle')
    time.sleep(sleep_timer)
    print(metric_updates)
    print(alert_updates)
    if len(metric_updates) == 0:
        test_collector.add_result('### Test 7 ### failed', TestResult.FAILED)
    else:
        for k, v in metric_updates.items():
            if len(v) < min_updates:
                print(f'found only {len(v)} updates for {k}, test step 7 failed')
                test_collector.add_result(f'### Test 7 Handle {k} ### failed', TestResult.FAILED)
            else:
                print(f'found {len(v)} updates for {k}, test step 7 ok')
                test_collector.add_result(f'### Test 7 Handle {k} ### passed', TestResult.PASSED)
    if len(alert_updates) == 0:
        test_collector.add_result('### Test 8 ### failed', TestResult.FAILED)
    else:
        for k, v in alert_updates.items():
            if len(v) < min_updates:
                print(f'found only {len(v)} updates for {k}, test step 8 failed')
                test_collector.add_result(f'### Test 8 Handle {k} ### failed', TestResult.FAILED)
            else:
                print(f'found {len(v)} updates for {k}, test step 8 ok')
                test_collector.add_result(f'### Test 8 Handle {k} ### passed', TestResult.PASSED)

    print('Test step 9: call SetString operation')
    setstring_operations = mdib.descriptions.NODETYPE.get(pm.SetStringOperationDescriptor, [])
    setst_handle = 'string.ch0.vmd1_sco_0'
    if len(setstring_operations) == 0:
        print('Test step 9(SetString) failed, no SetString operation found')
        test_collector.add_result('### Test 9 ### failed', TestResult.FAILED)
    else:
        for s in setstring_operations:
            if s.Handle != setst_handle:
                continue
            print(f'setString Op ={s}')
            try:
                fut = client.set_service_client.set_string(s.Handle, 'hoppeldipop')
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.InvocationInfo.InvocationState != InvocationState.FINISHED:
                        print(f'set string operation {s.Handle} did not finish with "Fin":{res}')
                        test_collector.add_result('### Test 9(SetString) ### failed', TestResult.FAILED)
                    else:
                        print(f'set string operation {s.Handle} ok:{res}')
                        test_collector.add_result('### Test 9(SetString) ### passed', TestResult.PASSED)
                except futures.TimeoutError:
                    print('timeout error')
                    test_collector.add_result('### Test 9(SetString) ### failed', TestResult.FAILED)
            except Exception as ex:  # noqa: BLE001
                print(f'Test 9(SetString): {ex}')
                test_collector.add_result('### Test 9(SetString) ### failed', TestResult.FAILED)

    print('Test step 9: call SetValue operation')
    setvalue_operations = mdib.descriptions.NODETYPE.get(pm.SetValueOperationDescriptor, [])
    setval_handle = 'numeric.ch0.vmd1_sco_0'
    if len(setvalue_operations) == 0:
        print('Test step 9 failed, no SetValue operation found')
        test_collector.add_result('### Test 9(SetValue) ### failed', TestResult.FAILED)
    else:
        for s in setvalue_operations:
            if s.Handle != setval_handle:
                continue
            print(f'setNumericValue Op ={s}')
            try:
                fut = client.set_service_client.set_numeric_value(s.Handle, Decimal('42'))
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.InvocationInfo.InvocationState != InvocationState.FINISHED:
                        print(f'set value operation {s.Handle} did not finish with "Fin":{res}')
                        test_collector.add_result('### Test 9(SetValue) ### failed', TestResult.FAILED)
                    else:
                        print(f'set value operation {s.Handle} ok:{res}')
                        test_collector.add_result('### Test 9(SetValue) ### passed', TestResult.PASSED)
                except futures.TimeoutError:
                    print('timeout error')
                    test_collector.add_result('### Test 9(SetValue) ### failed', TestResult.FAILED)
            except Exception as ex:  # noqa: BLE001
                print(f'Test 9(SetValue): {ex}')
                test_collector.add_result('### Test 9(SetValue) ### failed', TestResult.FAILED)

    print('Test step 9: call Activate operation')
    activate_operations = mdib.descriptions.NODETYPE.get(pm.ActivateOperationDescriptor, [])
    activate_handle = 'actop.vmd1_sco_0'
    if len(setstring_operations) == 0:
        print('Test step 9 failed, no Activate operation found')
        test_collector.add_result('### Test 9(Activate) ### failed', TestResult.FAILED)
    else:
        for s in activate_operations:
            if s.Handle != activate_handle:
                continue
            print(f'activate Op ={s}')
            try:
                fut = client.set_service_client.activate(s.Handle, 'hoppeldipop')
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.InvocationInfo.InvocationState != InvocationState.FINISHED:
                        print(f'activate operation {s.Handle} did not finish with "Fin":{res}')
                        test_collector.add_result('### Test 9(Activate) ### failed', TestResult.FAILED)
                    else:
                        print(f'activate operation {s.Handle} ok:{res}')
                        test_collector.add_result('### Test 9(Activate) ### passed', TestResult.PASSED)
                except futures.TimeoutError:
                    print('timeout error')
                    test_collector.add_result('### Test 9(Activate) ### failed', TestResult.FAILED)
            except Exception as ex:  # noqa: BLE001
                print(f'Test 9(Activate): {ex}')
                test_collector.add_result('### Test 9(Activate) ### failed', TestResult.FAILED)

    print('Test step 10: cancel all subscriptions')
    success = client._subscription_mgr.unsubscribe_all()  # noqa: SLF001
    if success:
        test_collector.add_result('### Test 10(unsubscribe) ### passed', TestResult.PASSED)
    else:
        test_collector.add_result('### Test 10(unsubscribe) ### failed', TestResult.FAILED)
    time.sleep(2)
    return test_collector


def main() -> TestCollector:
    """Execute reference tests."""
    xtra_log_config = os.getenv('ref_xtra_log_cnf')  # noqa: SIM112

    import json
    import logging.config

    with pathlib.Path(__file__).parent.joinpath('logging_default.json').open() as f:
        logging_setup = json.load(f)
    logging.config.dictConfig(logging_setup)
    if xtra_log_config is not None:
        with pathlib.Path(xtra_log_config).open() as f:
            logging_setup2 = json.load(f)
            logging.config.dictConfig(logging_setup2)
    comm_logger = commlog.DirectoryLogger(
        log_folder=r'c:\temp\sdc_refclient_commlog',
        log_out=True,
        log_in=True,
        broadcast_ip_filter=None,
    )
    if ENABLE_COMMLOG:
        for name in commlog.LOGGER_NAMES:
            logging.getLogger(name).setLevel(logging.DEBUG)
        comm_logger.start()
    results = run_ref_test()
    for r in results.test_messages:
        print(r)
    return results


if __name__ == '__main__':
    run_results = main()
    sys.exit(0 if run_results.overall_test_result is TestResult.PASSED else 1)
