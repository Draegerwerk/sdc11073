"""Implementation of reference consumer.

The reference consumer gets its parameters from environment variables:
- adapter_ip specifies which ip address shall be used
- ca_folder specifies where the communication certificates are located.
- ssl_passwd specifies an optional password for the certificates
- search_epr specifies the last characters of the endpoint reference of the device that the consumer shall connect to.
  It is not necessary to provide the full epr, just enough to be unique in the current network.

If a value is not provided as environment variable, the default value (see code below) will be used.
"""
from __future__ import annotations

import os
import time
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sdc11073 import observableproperties
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.consumer import SdcConsumer
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_qnames, msg_types

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.wsdiscovery.service import Service
    from sdc11073.pysoap.msgreader import ReceivedMessage

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


@dataclass
class ResultEntry:
    verdict: bool | None
    step: str
    info: str
    xtra: str

    def __str__(self):
        verdict_str = {None: 'no result', True: 'passed', False: 'failed'}
        return f'{self.step:6s}:{verdict_str[self.verdict]:10s} {self.info}{self.xtra}'


class ResultsCollector:
    def __init__(self):
        self._results: list[ResultEntry] = []

    def log_result(self, is_ok: bool | None, step: str, info: str, extra_info: str | None = None):
        xtra = f' ({extra_info}) ' if extra_info else ''
        self._results.append(ResultEntry(is_ok, step, info, xtra))

    def print_summary(self):
        print('\n### Summary ###')
        for r in self._results:
            print(r)

    @property
    def failed_count(self):
        return len([r for r in self._results if r.verdict is False])


class ConsumerMdibMethodsReferenceTest(ConsumerMdibMethods):
    def __init__(self, consumer_mdib, logger):
        super().__init__(consumer_mdib, logger)
        self.alert_condition_type_concept_updates: list[float] = []  # for test 5a.1
        self._last_alert_condition_type_concept_updates = time.monotonic()  # timestamp

        self.alert_condition_cause_remedy_updates: list[float] = []  # for test 5a.2
        self._last_alert_condition_cause_remedy_updates = time.monotonic()  # timestamp

        self.unit_of_measure_updates: list[float] = []  # for test 5a.3
        self._last_unit_of_measure_updates = time.monotonic()  # timestamp

    def _on_episodic_metric_report(self, received_message_data: ReceivedMessage):
        # test 4.1 : count numeric metric updates
        # The Reference Provider produces at least 5 numeric metric updates in 30 seconds
        super()._on_episodic_metric_report(received_message_data)

    def _on_description_modification_report(self, received_message_data: ReceivedMessage):
        """For Test 5a.1 check if the concept description of updated alert condition Type changed.
        For Test 5a.2 check if alert condition cause-remedy information changed.
        """
        cls = self._mdib.data_model.msg_types.DescriptionModificationReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        now = time.monotonic()
        dmt = self._mdib.sdc_definitions.data_model.msg_types.DescriptionModificationType
        for report_part in report.ReportPart:
            modification_type = report_part.ModificationType
            if modification_type == dmt.UPDATE:
                for descriptor_container in report_part.Descriptor:
                    if descriptor_container.is_alert_condition_descriptor:
                        old_descriptor = self._mdib.descriptions.handle.get_one(descriptor_container.Handle)
                        # test 5a.1
                        if descriptor_container.Type.ConceptDescription != old_descriptor.Type.ConceptDescription:
                            print(f'concept description {descriptor_container.Type.ConceptDescription} <=> '
                                  f'{old_descriptor.Type.ConceptDescription}')
                            self.alert_condition_type_concept_updates.append(
                                now - self._last_alert_condition_type_concept_updates)
                            self._last_alert_condition_type_concept_updates = now
                        # test 5a.2
                        # (CauseInfo is a list)
                        detected_5a2 = False
                        if len(descriptor_container.CauseInfo) != len(old_descriptor.CauseInfo):
                            print(f'RemedyInfo no. of CauseInfo {len(descriptor_container.CauseInfo)} <=> '
                                  f'{len(old_descriptor.CauseInfo)}')
                            detected_5a2 = True
                        else:
                            for i, cause_info in enumerate(descriptor_container.CauseInfo):
                                old_cause_info = old_descriptor.CauseInfo[i]
                                if cause_info.RemedyInfo != old_cause_info.RemedyInfo:
                                    print(f'RemedyInfo {cause_info.RemedyInfo} <=> '
                                          f'{old_cause_info.RemedyInfo}')
                                    detected_5a2 = True
                        if detected_5a2:
                            self.alert_condition_cause_remedy_updates.append(
                                now - self._last_alert_condition_cause_remedy_updates)
                            self._last_alert_condition_cause_remedy_updates = now
                    elif descriptor_container.is_metric_descriptor:
                        # test 5a.3
                        old_descriptor = self._mdib.descriptions.handle.get_one(descriptor_container.Handle)
                        if old_descriptor.Unit != descriptor_container.Unit:
                            self.unit_of_measure_updates.append(now - self._last_unit_of_measure_updates)
                            self._last_unit_of_measure_updates = now

        super()._on_description_modification_report(received_message_data)


def test_1b_resolve(wsd, my_service) -> (bool, str):
    """Send resolve and check response."""
    wsd.clear_remote_services()
    wsd._send_resolve(my_service.epr)
    time.sleep(3)
    if len(wsd._remote_services) == 0:
        return False, 'no response'
    elif len(wsd._remote_services) > 1:
        return False, 'multiple response'
    else:
        service = wsd._remote_services.get(my_service.epr)
        if service.epr != my_service.epr:
            return False, 'not the same epr'
        else:
            return True, 'resolve answered'


def connect_client(my_service: Service) -> SdcConsumer:
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


def test_min_updates_per_handle(updates_dict, min_updates, node_type_filter=None) -> (bool, str):  # True ok
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


def test_min_updates_for_type(updates_dict: dict, min_updates: int, q_name_list: list[QName]) -> (bool, str):  # True ok
    flat_list = []
    for v in updates_dict.values():
        flat_list.extend(v)
    matches = [x for x in flat_list if x.NODETYPE in q_name_list]
    if len(matches) >= min_updates:
        return True, ''
    return False, f'expect >= {min_updates}, got {len(matches)} out of {len(flat_list)}'


# def log_result(is_ok, result_list, step, info, extra_info=None):
#     xtra = f' ({extra_info}) ' if extra_info else ''
#     if is_ok:
#         result_list.append(f'{step} => passed {xtra}{info}')
#     else:
#         result_list.append(f'{step} => failed {xtra}{info}')


def run_ref_test(results_collector: ResultsCollector):
    # results = []
    print(f'using adapter address {adapter_ip}')
    print('Test step 1: discover device which endpoint ends with "{}"'.format(search_epr))
    wsd = WSDiscovery(adapter_ip)
    wsd.start()

    # 1. Device Discovery
    # a) The Reference Provider sends Hello messages
    # b) The Reference Provider answers to Probe and Resolve messages

    # Remark: 1a) is not testable because provider can't be forced to send a hello while this test is running.
    step = '1a'
    info = 'The Reference Provider sends Hello messages'
    results_collector.log_result(None, step, info, extra_info='not testable')

    step = '1b.1'
    info = 'The Reference Provider answers to Probe messages'
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
    results_collector.log_result(True, step, info)

    step = '1b.2'
    info = 'The Reference Provider answers to Resolve messages'
    print('Test step 1b: send resolve and check response')
    is_ok, txt = test_1b_resolve(wsd, my_service)
    results_collector.log_result(is_ok, step, info, extra_info=txt)

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
        results_collector.log_result(client.host_description is not None, step, info)
    except:
        print(traceback.format_exc())
        results_collector.log_result(False, step, info)
        return  # results

    step = '2b.1'
    info = 'the Reference Provider grants subscriptions of at most 15 seconds'
    now = time.time()
    durations = [s.expires_at - now for s in client.subscription_mgr.subscriptions.values()]
    print(f'subscription durations = {durations}')
    results_collector.log_result(max(durations) <= 15, step, info)
    step = '2b.2'
    info = 'the Reference Provider grants subscriptions of at most 15 seconds (renew)'
    subscription = list(client.subscription_mgr.subscriptions.values())[0]
    granted = subscription.renew(30000)
    print(f'renew granted = {granted}')
    results_collector.log_result(max(durations) <= 15, step, info)

    # 3. Request Response
    # a) The Reference Provider answers to GetMdib
    # b) The Reference Provider answers to GetContextStates messages
    # b.1) The Reference Provider provides at least one location context state
    step = '3a'
    info = 'The Reference Provider answers to GetMdib'
    print(step, info)
    try:
        mdib = ConsumerMdib(client, extras_cls=ConsumerMdibMethodsReferenceTest)
        mdib.init_mdib()  # throws an exception if provider did not answer to GetMdib
        results_collector.log_result(True, step, info)
    except:
        print(traceback.format_exc())
        results_collector.log_result(False, step, info)
        # results.append(f'{step} => failed')
        return  # results

    step = '3b'
    info = 'The Reference Provider answers to GetContextStates messages'
    context_service = client.context_service_client
    if context_service is None:
        results_collector.log_result(False, step, info, extra_info='no context service')
    else:
        try:
            states = context_service.get_context_states().result.ContextState
            results_collector.log_result(True, step, info)
        except:
            print(traceback.format_exc())
            results_collector.log_result(False, step, info, extra_info='exception')
        step = '3b.1'
        info = 'The Reference Provider provides at least one location context state'
        loc_states = [s for s in states if s.NODETYPE == pm_qnames.LocationContextState]
        results_collector.log_result(len(loc_states) > 0, step, info)

    # 4 State Reports
    # a) The Reference Provider produces at least 5 numeric metric updates in 30 seconds
    # b) The Reference Provider produces at least 5 string metric updates (StringMetric or EnumStringMetric) in 30 seconds
    # c) The Reference Provider produces at least 5 alert condition updates (AlertCondition or LimitAlertCondition) in 30 seconds
    # d) The Reference Provider produces at least 5 alert signal updates in 30 seconds
    # e) The Reference Provider provides alert system self checks in accordance to the periodicity defined in the MDIB (at least every 10 seconds)
    # f) The Reference Provider provides 3 waveforms (RealTimeSampleArrayMetric) x 10 messages per second x 100 samples per message
    # g) The Reference Provider provides changes for the following components:
    #   * At least 5 Clock or Battery object updates in 30 seconds (Component report)
    #   * At least 5 MDS or VMD updates in 30 seconds (Component report)
    # g) The Reference Provider provides changes for the following operational states:
    #    At least 5 Operation updates in 30 seconds; enable/disable operations; some different than the ones mentioned above (Operational State Report)"""

    # setup data collectors for next test steps
    numeric_metric_updates = defaultdict(list)
    string_metric_updates = defaultdict(list)
    alert_condition_updates = defaultdict(list)
    alert_signal_updates = defaultdict(list)
    alert_system_updates = defaultdict(list)
    component_updates = defaultdict(list)
    waveform_updates = defaultdict(list)
    operational_state_updates = defaultdict(list)
    description_updates = []

    def on_metric_updates(metrics_by_handle):
        """Callback for all metric state updates.

        Writes to numeric_metric_updates or string_metric_updates, depending on type of state.
        """
        for k, v in metrics_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            if v.NODETYPE == pm_qnames.NumericMetricState:
                numeric_metric_updates[k].append(v)
            elif v.NODETYPE == pm_qnames.StringMetricState:
                string_metric_updates[k].append(v)

    def on_alert_updates(alerts_by_handle):
        """Callback for all alert state updates.

        Writes to alert_condition_updates, alert_signal_updates or alert_system_updates, depending on type of state.
        """
        for k, v in alerts_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            if v.is_alert_condition:
                alert_condition_updates[k].append(v)
            elif v.is_alert_signal:
                alert_signal_updates[k].append(v)
            elif v.NODETYPE == pm_qnames.AlertSystemState:
                alert_system_updates[k].append(v)

    def on_component_updates(components_by_handle):
        """Callback for all component state updates.

        Writes to component_updates .
        """
        for k, v in components_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            component_updates[k].append(v)

    def on_waveform_updates(waveforms_by_handle):
        """Callback for all waveform state updates.

        Writes to waveform_updates .
        """
        for k, v in waveforms_by_handle.items():
            waveform_updates[k].append(v)

    def on_description_modification(description_modification_report):
        """Callback for all description modification updates.

        Writes to description_updates .
        """
        print('on_description_modification')
        description_updates.append(description_modification_report)

    def on_operational_state_updates(operational_states_by_handle):
        """Callback for all operational state updates.

        Writes to operational_state_updates .
        """
        for k, v in operational_states_by_handle.items():
            print(f'State {v.NODETYPE.localname} {v.DescriptorHandle}')
            operational_state_updates[k].append(v)

    observableproperties.bind(mdib, metrics_by_handle=on_metric_updates)
    observableproperties.bind(mdib, alert_by_handle=on_alert_updates)
    observableproperties.bind(mdib, component_by_handle=on_component_updates)
    observableproperties.bind(mdib, waveform_by_handle=on_waveform_updates)
    observableproperties.bind(mdib, description_modifications=on_description_modification)
    observableproperties.bind(mdib, operation_by_handle=on_operational_state_updates)

    # now collect reports
    sleep_timer = 30
    min_updates = 5
    print(f'will wait for {sleep_timer} seconds now, expecting at least {min_updates} updates per Handle')
    time.sleep(sleep_timer)

    # now check report count
    step = '4a'
    info = 'count numeric metric state updates'
    print(step, info)
    is_ok, result = test_min_updates_per_handle(numeric_metric_updates, min_updates)
    results_collector.log_result(is_ok, step, info)

    step = '4b'
    info = 'count string metric state updates'
    print(step)
    is_ok, result = test_min_updates_per_handle(string_metric_updates, min_updates)
    results_collector.log_result(is_ok, step, info)

    step = '4c'
    info = 'count alert condition updates'
    print(step)
    is_ok, result = test_min_updates_per_handle(alert_condition_updates, min_updates)
    results_collector.log_result(is_ok, step, info)

    step = '4d'
    info = ' count alert signal updates'
    print(step, info)
    is_ok, result = test_min_updates_per_handle(alert_signal_updates, min_updates)
    results_collector.log_result(is_ok, step, info)

    step = '4e'
    info = 'count alert system self checks'
    is_ok, result = test_min_updates_per_handle(alert_system_updates, min_updates)
    results_collector.log_result(is_ok, step, info)

    step = '4f'
    info = 'count waveform updates'
    # 3 waveforms (RealTimeSampleArrayMetric) x 10 messages per second x 100 samples per message
    print(step, info)
    is_ok, result = test_min_updates_per_handle(waveform_updates, min_updates)
    results_collector.log_result(is_ok, step, info + ' notifications per second')
    results_collector.log_result(len(waveform_updates) >= 3, step, info + ' number of waveforms')

    expected_samples = 1000 * sleep_timer * 0.9
    for handle, reports in waveform_updates.items():
        notifications = [n for n in reports if n.MetricValue is not None]
        samples = sum([len(n.MetricValue.Samples) for n in notifications])
        if samples < expected_samples:
            results_collector.log_result(False, step,
                                         info + f' waveform {handle} has {samples} samples, expecting {expected_samples}')
        else:
            results_collector.log_result(True, step, info + f' waveform {handle} has {samples} samples')

    pm = mdib.data_model.pm_names
    pm_types = mdib.data_model.pm_types

    step = '4g.1'
    info = 'count battery or clock updates'
    print(step, info)
    is_ok, result = test_min_updates_for_type(component_updates,
                                              min_updates,
                                              [pm.BatteryState, pm.ClockState])
    results_collector.log_result(is_ok, step, info)

    step = '4g.2'
    info = 'count VMD or MDS updates'
    print(step, info)
    is_ok, result = test_min_updates_for_type(component_updates,
                                              min_updates,
                                              [pm.VmdState, pm.MdsState])
    results_collector.log_result(is_ok, step, info)

    step = '4h'
    info = 'Enable/Disable operations'
    print(step, info)
    is_ok, result = test_min_updates_for_type(operational_state_updates,
                                              min_updates,
                                              [pm.SetValueOperationState,
                                               pm.SetStringOperationState,
                                               pm.ActivateOperationState,
                                               pm.SetContextStateOperationState,
                                               pm.SetMetricStateOperationState,
                                               pm.SetComponentStateOperationState,
                                               pm.SetAlertStateOperationState])
    results_collector.log_result(is_ok, step, info)

    # 5 Description Modifications:
    # a) The Reference Provider produces at least 1 update every 10 seconds comprising
    #     * Update Alert condition concept description of Type
    #     * Update Alert condition cause-remedy information
    #     * Update Unit of measure (metrics)
    # b)  The Reference Provider produces at least 1 insertion followed by a deletion every 10 seconds comprising
    #     * Insert a VMD including Channels including metrics (inserted VMDs/Channels/Metrics are required to have
    #       a new handle assigned on each insertion such that containment tree entries are not recycled).
    #       (Tests for the handling of re-insertion of previously inserted objects should be tested additionally)
    #     * Remove the VMD
    step = '5a.1'
    info = 'Update Alert condition concept description of Type'
    print(step, info)
    # verify only that there are Alert Condition Descriptors updated
    updates = mdib.xtra.alert_condition_type_concept_updates
    if not updates:
        results_collector.log_result(False, step, info, 'no updates')
    else:
        max_diff = max(updates)
        if max_diff > 10:
            results_collector.log_result(False, step, info, f'max dt={max_diff}')
        else:
            results_collector.log_result(True, step, info, f'{len(updates) - 1} updates, max diff= {max_diff:.1f}')

    step = '5a.2'
    info = 'Update Alert condition cause-remedy information'
    print(step, info)
    # verify only that there are remedy infos updated
    updates = mdib.xtra.alert_condition_cause_remedy_updates
    if not updates:
        results_collector.log_result(False, step, info, 'no updates')
    else:
        max_diff = max(updates)
        if max_diff > 10:
            results_collector.log_result(False, step, info, f'{updates} => max dt={max_diff}')
        else:
            results_collector.log_result(True, step, info, f'{len(updates) - 1} updates, max diff= {max_diff:.1f}')

    step = '5a.3'
    info = 'Update Unit of measure'
    print(step, info)
    updates = mdib.xtra.unit_of_measure_updates
    if not updates:
        results_collector.log_result(False, step, info, 'no updates')
    else:
        max_diff = max(updates)
        if max_diff > 10:
            results_collector.log_result(False, step, info, f'max dt={max_diff}')
        else:
            results_collector.log_result(True, step, info, f'{len(updates) - 1} updates, max diff= {max_diff:.1f}')

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
    results_collector.log_result(add_found, step, info, 'add')
    results_collector.log_result(rm_found, step, info, 'remove')

    # 6 Operation invocation
    # a) (removed)
    # b) SetContextState:
    #     * Payload: 1 Patient Context
    #     * Context state is added to the MDIB including context association and validation
    #     * If there is an associated context already, that context shall be disassociated
    #         * Handle and version information is generated by the provider
    #     * In order to avoid infinite growth of patient contexts, older contexts are allowed to be removed from the MDIB
    #       (=ContextAssociation=No)
    # c) SetValue: Immediately answers with "finished"
    #     * Finished has to be sent as a report in addition to the response =>
    # d) SetString: Initiates a transaction that sends Wait, Start and Finished
    # e) SetMetricStates:
    #     * Payload: 2 Metric States (settings; consider alert limits)
    #     * Immediately sends finished
    #     * Action: Alter values of metrics

    step = '6b'
    info = 'SetContextState'
    print(step, info)
    # patients = mdib.context_states.NODETYPE.get(pm.PatientContextState, [])
    patient_context_descriptors = mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor, [])
    generated_family_names = []
    if len(patient_context_descriptors) == 0:
        results_collector.log_result(False, step, info, extra_info='no PatientContextDescriptor')
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
                results_collector.log_result(False, step, info, extra_info='no patients found')
            else:
                all_ok = True
                for patient in patients:
                    if patient.CoreData.Familyname in generated_family_names:
                        if patient.ContextAssociation != pm_types.ContextAssociation.ASSOCIATED:
                            results_collector.log_result(False, step, info,
                                                         extra_info=f'new patient {patient.CoreData.Familyname} is {patient.ContextAssociation}')
                            all_ok = False
                    else:
                        if patient.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED:
                            results_collector.log_result(False, step, info,
                                                         extra_info=f'old patient {patient.CoreData.Familyname} is {patient.ContextAssociation}')
                            all_ok = False
                results_collector.log_result(all_ok, step, info)
        except Exception as ex:
            print(traceback.format_exc())
            results_collector.log_result(False, step, info, ex)

    step = '6c'
    info = 'SetValue: Immediately answers with "finished"'
    print(step, info)
    subscriptions = client.subscription_mgr.subscriptions.values()
    operation_invoked_subscriptions = [subscr for subscr in subscriptions
                                       if 'OperationInvokedReport' in subscr.short_filter_string]
    if len(operation_invoked_subscriptions) == 0:
        results_collector.log_result(False, step, info, 'OperationInvokedReport not subscribed, cannot test')
    elif len(operation_invoked_subscriptions) > 1:
        results_collector.log_result(False, step, info,
                                     f'found {len(operation_invoked_subscriptions)} OperationInvokedReport subscribed, cannot test')
    else:
        try:
            operations = client.mdib.descriptions.NODETYPE.get(pm_qnames.SetValueOperationDescriptor, [])
            my_ops = [op for op in operations if op.Type.Code == "67108888"]
            if len(my_ops) != 1:
                results_collector.log_result(False, step, info, f'found {len(my_ops)} operations with code "67108888"')
            else:
                operation = my_ops[0]
                future_object = client.set_service_client.set_numeric_value(operation.Handle, Decimal(42))
                operation_result = future_object.result()
                if len(operation_result.report_parts) == 0:
                    results_collector.log_result(False, step, info, 'no notification')
                elif len(operation_result.report_parts) > 1:
                    results_collector.log_result(False, step, info,
                                                 f'got {len(operation_result.report_parts)} notifications, expect only one')
                else:
                    results_collector.log_result(True, step, info,
                                                 f'got {len(operation_result.report_parts)} notifications')
                if operation_result.InvocationInfo.InvocationState != msg_types.InvocationState.FINISHED:
                    results_collector.log_result(False, step, info,
                                                 f'got result {operation_result.InvocationInfo.InvocationState} '
                                                 f'{operation_result.InvocationInfo.InvocationError} '
                                                 f'{operation_result.InvocationInfo.InvocationErrorMessage}')
        except Exception as ex:
            print(traceback.format_exc())
            results_collector.log_result(False, step, info, ex)

    step = '6d'
    info = 'SetString: Initiates a transaction that sends Wait, Start and Finished'
    print(step, info)
    try:
        operations = client.mdib.descriptions.NODETYPE.get(pm_qnames.SetStringOperationDescriptor, [])
        my_ops = [op for op in operations if op.Type.Code == "67108889"]
        if len(my_ops) != 1:
            results_collector.log_result(False, step, info, f'found {len(my_ops)} operations with code "67108889"')
        else:
            operation = my_ops[0]
            future_object = client.set_service_client.set_string(operation.Handle, 'STANDBY')
            operation_result = future_object.result()
            if len(operation_result.report_parts) < 3:
                results_collector.log_result(False, step, info,
                                             f'only {len(operation_result.report_parts)} notification(s)')
            elif len(operation_result.report_parts) >= 3:
                # check order of operation invoked reports (simple expectation, there could be multiple WAIT in theory)
                expectation = [msg_types.InvocationState.WAIT,
                               msg_types.InvocationState.START,
                               msg_types.InvocationState.FINISHED]
                inv_states = [p.InvocationInfo.InvocationState for p in operation_result.report_parts]
                if inv_states != expectation:
                    results_collector.log_result(False, step, info, f'wrong order {inv_states}')
                else:
                    results_collector.log_result(True, step, info,
                                                 f'got {len(operation_result.report_parts)} notifications')
            if operation_result.InvocationInfo.InvocationState != msg_types.InvocationState.FINISHED:
                results_collector.log_result(False, step, info,
                                             f'got result {operation_result.InvocationInfo.InvocationState} '
                                             f'{operation_result.InvocationInfo.InvocationError} '
                                             f'{operation_result.InvocationInfo.InvocationErrorMessage}')

    except Exception as ex:
        print(traceback.format_exc())
        results_collector.log_result(False, step, info, ex)

    step = '6e'
    info = 'SetMetricStates Immediately answers with finished'
    print(step, info)
    try:
        operations = client.mdib.descriptions.NODETYPE.get(pm_qnames.SetMetricStateOperationDescriptor, [])
        my_ops = [op for op in operations if op.Type.Code == "67108890"]
        if len(my_ops) != 1:
            results_collector.log_result(False, step, info, f'found {len(my_ops)} operations with code "67108890"')
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
            future_object = client.set_service_client.set_metric_state(operation.Handle,
                                                                       [proposed_metric_state1, proposed_metric_state2])
            operation_result = future_object.result()
            if len(operation_result.report_parts) == 0:
                results_collector.log_result(False, step, info, 'no notification')
            elif len(operation_result.report_parts) > 1:
                results_collector.log_result(False, step, info,
                                             f'got {len(operation_result.report_parts)} notifications, expect only one')
            else:
                results_collector.log_result(True, step, info,
                                             f'got {len(operation_result.report_parts)} notifications')
            if operation_result.InvocationInfo.InvocationState != msg_types.InvocationState.FINISHED:
                results_collector.log_result(False, step, info,
                                             f'got result {operation_result.InvocationInfo.InvocationState} '
                                             f'{operation_result.InvocationInfo.InvocationError} '
                                             f'{operation_result.InvocationInfo.InvocationErrorMessage}')
    except Exception as ex:
        print(traceback.format_exc())
        results_collector.log_result(False, step, info, ex)

    step = '7'
    info = 'Graceful shutdown (at least subscriptions are ended; optionally Bye is sent)'
    try:
        success = client._subscription_mgr.unsubscribe_all()
        results_collector.log_result(success, step, info)
    except Exception as ex:
        print(traceback.format_exc())
        results_collector.log_result(False, step, info, ex)
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

    results = ResultsCollector()

    run_ref_test(results)
    results.print_summary()
    if results.failed_count:
        exit(-1)
    exit(0)
