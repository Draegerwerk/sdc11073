"""Tests metric updates."""

from __future__ import annotations

import collections
import functools
import logging
import queue
import threading
import time
from typing import TYPE_CHECKING

from pat.consumer_tests import result_collector
from sdc11073.observableproperties import observables
from sdc11073.xml_types import pm_qnames

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sdc11073 import xml_utils
    from sdc11073.mdib import ConsumerMdib, descriptorcontainers, statecontainers


__STEP__ = '4'
logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def _count_states_occurrences(  # noqa: PLR0913
    step: str,
    update_queue: queue.Queue,
    updates_required_count: int,
    node_types: Iterable[xml_utils.QName],
    collected_updates_within_timeout_event: threading.Event,
    states_by_handle: dict[str, statecontainers.AbstractStateContainer],
):
    for state in states_by_handle.values():
        if state.NODETYPE in node_types:
            logger.debug(
                'State "%s" of type "%s" received',
                state.DescriptorHandle,
                state.NODETYPE,
                extra={'step': step},
            )
            update_queue.put_nowait(state)
            if update_queue.qsize() >= updates_required_count:
                collected_updates_within_timeout_event.set()


def _verify_state_updates_in_time(
    mdib: ConsumerMdib,
    step: str,
    node_types: Iterable[xml_utils.QName],
    updates_required_count: int,
    timeout: float,
) -> bool:
    test_result = False
    collected_updates_within_timeout = threading.Event()

    update_queue = queue.Queue()
    observer = functools.partial(
        _count_states_occurrences,
        step,
        update_queue,
        updates_required_count,
        node_types,
        collected_updates_within_timeout,
    )

    if all(
        node_type in (pm_qnames.NumericMetricState, pm_qnames.StringMetricState, pm_qnames.EnumStringMetricState)
        for node_type in node_types
    ):
        mdib_observer = 'metrics_by_handle'
    elif all(
        node_type in (pm_qnames.AlertConditionState, pm_qnames.LimitAlertConditionState, pm_qnames.AlertSignalState)
        for node_type in node_types
    ):
        mdib_observer = 'alert_by_handle'
    elif all(node_type in (pm_qnames.RealTimeSampleArrayMetricState,) for node_type in node_types):
        mdib_observer = 'waveform_by_handle'
    elif all(
        node_type in (pm_qnames.ClockState, pm_qnames.BatteryState, pm_qnames.MdsState, pm_qnames.VmdState)
        for node_type in node_types
    ):
        mdib_observer = 'component_by_handle'
    elif all(
        node_type
        in (
            pm_qnames.ActivateOperationState,
            pm_qnames.SetAlertStateOperationState,
            pm_qnames.SetComponentStateOperationState,
            pm_qnames.SetContextStateOperationState,
            pm_qnames.SetMetricStateOperationState,
            pm_qnames.SetStringOperationState,
            pm_qnames.SetValueOperationState,
        )
        for node_type in node_types
    ):
        mdib_observer = 'operation_by_handle'
    else:
        msg = f'Unknown node type(s) {", ".join(str(node_type) for node_type in node_types)}'
        raise ValueError(msg)
    with observables.bound_context(mdib, **{mdib_observer: observer}):
        if collected_updates_within_timeout.wait(timeout):
            logger.info(
                'The reference provider produced state updates "%s/%s" which is enough within %s seconds.',
                update_queue.qsize(),
                updates_required_count,
                timeout,
                extra={'step': step},
            )
        else:
            result_collector.ResultCollector.log_failure(
                step=step,
                message=f'The reference provider produced state updates '
                f'"{update_queue.qsize()}/{updates_required_count}" which is not enough within '
                f'{timeout} seconds.',
            )
    return test_result


def test_4a(mdib: ConsumerMdib):
    """The Reference Provider produces at least 5 numeric metric updates in 30 seconds."""
    _verify_state_updates_in_time(
        mdib=mdib,
        step=f'{__STEP__}a',
        node_types=(pm_qnames.NumericMetricState,),
        updates_required_count=5,
        timeout=30.0,
    )


def test_4b(mdib: ConsumerMdib):
    """The Reference Provider produces at least 5 string metric updates (StringMetric or EnumStringMetric) in 30 seconds."""  # noqa: E501, W505
    _verify_state_updates_in_time(
        mdib=mdib,
        step=f'{__STEP__}b',
        node_types=(pm_qnames.StringMetricState, pm_qnames.EnumStringMetricState),
        updates_required_count=5,
        timeout=30.0,
    )


def test_4c(mdib: ConsumerMdib):
    """The Reference Provider produces at least 5 alert condition updates (AlertCondition or LimitAlertCondition) in 30 seconds."""  # noqa: E501, W505
    _verify_state_updates_in_time(
        mdib=mdib,
        step=f'{__STEP__}c',
        node_types=(pm_qnames.AlertConditionState, pm_qnames.LimitAlertConditionState),
        updates_required_count=5,
        timeout=30.0,
    )


def test_4d(mdib: ConsumerMdib):
    """The Reference Provider produces at least 5 alert signal updates in 30 seconds."""
    _verify_state_updates_in_time(
        mdib=mdib,
        step=f'{__STEP__}d',
        node_types=(pm_qnames.AlertSignalState,),
        updates_required_count=5,
        timeout=30.0,
    )


def _on_alert_system_update(  # noqa: PLR0913
    step: str,
    descriptor_handle_to_observe: str,
    first_update: threading.Event,
    second_update: threading.Event,
    last_self_check_count: dict[str, int | None],
    alerts_by_handle: dict,
):
    if descriptor_handle_to_observe in alerts_by_handle:
        state: statecontainers.AlertSystemStateContainer = alerts_by_handle[descriptor_handle_to_observe]
        if state.SelfCheckCount is None:
            # TODO: what to do if SelfCheckCount is None?  # noqa: FIX002, TD002, TD003
            logger.warning(
                'AlertSystemStateContainer "%s" has no SelfCheckCount',
                descriptor_handle_to_observe,
                extra={'step': step},
            )
            return
        logger.debug(
            'AlertSystemStateContainer "%s" has SelfCheckCount "%d"',
            descriptor_handle_to_observe,
            state.SelfCheckCount,
            extra={'step': step},
        )
        previous_count = last_self_check_count.get('value')
        if previous_count is None:
            last_self_check_count['value'] = state.SelfCheckCount
            first_update.set()
            return
        # ignore notifications keeping the counter constant (e.g. alert-condition churn) so timing uses real self checks
        if state.SelfCheckCount != previous_count:
            last_self_check_count['value'] = state.SelfCheckCount
            if not first_update.is_set():
                first_update.set()
            else:
                second_update.set()


def test_4e(mdib: ConsumerMdib):  # noqa: C901
    """The Reference Provider provides alert system self checks in accordance to the periodicity defined in the MDIB (at least every 10 seconds)."""  # noqa: E501, W505
    step = f'{__STEP__}e'
    max_self_check_period = 10
    alert_systems: Sequence[descriptorcontainers.AlertSystemDescriptorContainer] = mdib.descriptions.NODETYPE.get(
        pm_qnames.AlertSystemDescriptor,
        [],
    )
    if not alert_systems:
        result_collector.ResultCollector.log_failure(
            step=step,
            message='The reference provider does not provide an AlertSystemDescriptor.',
        )
        return
    if all(alert_system.SelfCheckPeriod is None for alert_system in alert_systems):
        result_collector.ResultCollector.log_failure(
            step=step,
            message='The reference provider does not provide SelfCheckPeriod in any AlertSystemDescriptor.',
        )
        return
    for alert_system in alert_systems:
        if alert_system.SelfCheckPeriod is None:
            logger.debug('Skipping AlertSystemDescriptor "%s" without SelfCheckPeriod', alert_system.Handle, extra={'step': step})
            continue
        if alert_system.SelfCheckPeriod <= max_self_check_period:
            result_collector.ResultCollector.log_success(
                step=step,
                message=f'The AlertSystemDescriptor {alert_system.Handle} has a SelfCheckPeriod of '
                f'{alert_system.SelfCheckPeriod} seconds, which is at most {max_self_check_period} seconds.',
            )
        else:
            result_collector.ResultCollector.log_failure(
                step=step,
                message=f'The AlertSystemDescriptor {alert_system.Handle} has a SelfCheckPeriod of '
                f'{alert_system.SelfCheckPeriod} seconds, which is more than {max_self_check_period} seconds.',
            )

    network_delay = 1  # seconds, to allow for network delays and processing time
    for alert_system in alert_systems:
        if alert_system.SelfCheckPeriod is None:
            logger.info(
                'Skipping AlertSystemDescriptor "%s" without SelfCheckPeriod',
                alert_system.Handle,
                extra={'step': step},
            )
            continue
        first_update = threading.Event()
        second_update = threading.Event()
        # store last SelfCheckCount in a mutable container so the callback can update it across invocations
        # otherwise 4e latches onto alert updates that do not advance the counter and reports a 0s interval
        self_check_counter = {'value': None}
        observer = functools.partial(
            _on_alert_system_update,
            step,
            alert_system.Handle,
            first_update,
            second_update,
            self_check_counter,
        )
        timeout = alert_system.SelfCheckPeriod + network_delay
        with observables.bound_context(mdib, alert_by_handle=observer):
            if not first_update.wait(timeout):
                result_collector.ResultCollector.log_failure(
                    step=step,
                    message=f'The reference provider did not produce alert system updates for '
                    f'"{alert_system.Handle}" within {alert_system.SelfCheckPeriod} (+{network_delay}s network delay) '
                    f'seconds.',
                )
                continue
            start = time.perf_counter()
            if not second_update.wait(timeout):
                result_collector.ResultCollector.log_failure(
                    step=step,
                    message=f'The reference provider did not produce a second alert system update for '
                    f'"{alert_system.Handle}" within {alert_system.SelfCheckPeriod} (+{network_delay}s network delay) '
                    f'seconds.',
                )
                continue
            duration = time.perf_counter() - start
            if alert_system.SelfCheckPeriod - network_delay <= duration <= alert_system.SelfCheckPeriod + network_delay:
                result_collector.ResultCollector.log_success(
                    step=step,
                    message=f'The reference provider produced alert system self check updates in accordance to the '
                    f'periodicity defined in the MDIB (but at most {max_self_check_period} seconds) for '
                    f'"{alert_system.Handle}" within {alert_system.SelfCheckPeriod} '
                    f'(+{network_delay}s network delay) seconds.',
                )
            else:
                result_collector.ResultCollector.log_failure(
                    step=step,
                    message=f'The reference provider produced alert system self check updates for '
                    f'"{alert_system.Handle}" with a duration of {duration:.2f} seconds, which is not in '
                    f'accordance to the periodicity defined in the MDIB {alert_system.SelfCheckPeriod} seconds '
                    f'(and at most {max_self_check_period} seconds).',
                )


def _on_waveform_updates(waveform_updates: dict, waveforms_by_handle: dict):
    for handle, state in waveforms_by_handle.items():
        waveform_updates[handle].append(state)


def _verify_waveform_tests(  # noqa: PLR0913
    mdib: ConsumerMdib,
    step: str,
    at_least_waveform_descriptors: int,
    waveform_updates_per_second: int,
    samples_per_message: int,
    timeout: float,
    network_delay: float,
):
    waveform_descriptors = mdib.descriptions.NODETYPE.get(pm_qnames.RealTimeSampleArrayMetricDescriptor)
    if len(waveform_descriptors) < at_least_waveform_descriptors:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'The reference provider does not provide at least {at_least_waveform_descriptors} '
            f'RealTimeSampleArrayMetricDescriptor, but {len(waveform_descriptors)}.',
        )
        return

    waveform_updates: dict[str, list[statecontainers.RealTimeSampleArrayMetricStateContainer]] = (
        collections.defaultdict(list)
    )
    observer = functools.partial(_on_waveform_updates, waveform_updates)
    with observables.bound_context(mdib, waveform_by_handle=observer):
        time.sleep(timeout + network_delay)
    if len(waveform_updates) < at_least_waveform_descriptors:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'The reference provider did not produce updates for at least {at_least_waveform_descriptors} '
            f'waveforms, but {len(waveform_updates)}.',
        )

    # Track how many waveforms meet the criteria
    # check for equality is not reliable due to network delays
    waveforms_with_sufficient_updates = sum(
        1 for updates in waveform_updates.values() if len(updates) >= waveform_updates_per_second * timeout
    )
    # check if all updates have at least samples_per_message samples.
    # due to network delays, we cannot guarantee that all updates have exactly samples_per_message samples
    waveforms_with_sufficient_samples = sum(
        1
        for updates in waveform_updates.values()
        if all(len(update.MetricValue.Samples) >= samples_per_message for update in updates)
    )

    # Final check: at least the required number of waveforms must meet both criteria
    if waveforms_with_sufficient_updates >= at_least_waveform_descriptors:
        result_collector.ResultCollector.log_success(
            step=step,
            message=f'At least {at_least_waveform_descriptors} waveforms produced sufficient updates '
            f'({waveforms_with_sufficient_updates} waveforms met the criteria).',
        )
    else:
        for handle, updates in waveform_updates.items():
            if len(updates) < waveform_updates_per_second * timeout:
                result_collector.ResultCollector.log_failure(
                    step=step,
                    message=f'The reference provider did not produce enough updates for waveform {handle}, '
                    f'expected {waveform_updates_per_second * timeout} but only {len(updates)} updates were received.',
                )
            else:
                result_collector.ResultCollector.log_success(
                    step=step,
                    message=f'The reference provider produced enough updates for waveform {handle}, '
                    f'{len(updates)} updates were received.',
                )

    if waveforms_with_sufficient_samples >= at_least_waveform_descriptors:
        result_collector.ResultCollector.log_success(
            step=step,
            message=f'At least {at_least_waveform_descriptors} waveforms produced sufficient samples per message '
            f'({waveforms_with_sufficient_samples} waveforms met the criteria).',
        )
    else:
        for handle, updates in waveform_updates.items():
            if all(len(update.MetricValue.Samples) >= samples_per_message for update in updates):
                result_collector.ResultCollector.log_success(
                    step=step,
                    message=f'The reference provider produced updates with at least {samples_per_message} samples '
                    f'for waveform {handle}.',
                )
            else:
                updates_not_sufficient = [
                    len(update.MetricValue.Samples)
                    for update in updates
                    if len(update.MetricValue.Samples) != samples_per_message
                ]

                result_collector.ResultCollector.log_failure(
                    step=step,
                    message=f'The reference provider did not produce updates with at least {samples_per_message} '
                    f'samples for waveform {handle}, but some updates have a different number of samples '
                    f'{", ".join(f"{samples}/{samples_per_message}" for samples in updates_not_sufficient)}.',
                )


def test_4f(mdib: ConsumerMdib, network_delay: float | None = None):  # noqa: PT028
    """The Reference Provider provides 3 waveforms (RealTimeSampleArrayMetric) x 10 messages per second x 100 samples per message."""  # noqa: E501, W505
    _verify_waveform_tests(
        mdib=mdib,
        step=f'{__STEP__}f',
        at_least_waveform_descriptors=3,
        waveform_updates_per_second=10,
        samples_per_message=100,
        # seconds, to allow for the updates to arrive
        timeout=1.0,
        # seconds, to allow for network delays and processing time
        network_delay=network_delay if network_delay is not None else 0.1,
    )


def test_4g(mdib: ConsumerMdib):
    """The Reference Provider The Reference Provider provides changes for the following components:
    - At least 5 Clock or Battery object updates in 30 seconds (Component report)
    - At least 5 MDS or VMD updates in 30 seconds (Component report)
    """  # noqa: D205, D400, D415
    step = f'{__STEP__}g'

    _verify_state_updates_in_time(
        mdib=mdib,
        step=step,
        node_types=(pm_qnames.ClockState, pm_qnames.BatteryState),
        updates_required_count=5,
        timeout=30.0,
    )
    _verify_state_updates_in_time(
        mdib=mdib,
        step=step,
        node_types=(pm_qnames.MdsState, pm_qnames.VmdState),
        updates_required_count=5,
        timeout=30.0,
    )


def test_4h(mdib: ConsumerMdib):
    """The Reference Provider The Reference Provider provides changes for the following operational states:

    - At least 5 Operation updates in 30 seconds; enable/disable operations; some different than the ones mentioned above (Operational State Report)
    """  # noqa: D400, D415, E501, W505
    _verify_state_updates_in_time(
        mdib=mdib,
        step=f'{__STEP__}h',
        node_types=(
            pm_qnames.ActivateOperationState,
            pm_qnames.SetAlertStateOperationState,
            pm_qnames.SetComponentStateOperationState,
            pm_qnames.SetContextStateOperationState,
            pm_qnames.SetMetricStateOperationState,
            pm_qnames.SetStringOperationState,
            pm_qnames.SetValueOperationState,
        ),
        updates_required_count=5,
        timeout=30.0,
    )


def test_4i(mdib: ConsumerMdib, network_delay: float | None = None):  # noqa: PT028
    """The Reference Provider provides 1 waveform (RealTimeSampleArrayMetric) x 2 messages per second x 50 samples per message (reduced amount of messages per second to cover slow networks)."""  # noqa: E501, W505
    _verify_waveform_tests(
        mdib=mdib,
        step=f'{__STEP__}i',
        at_least_waveform_descriptors=1,
        waveform_updates_per_second=2,
        samples_per_message=50,
        # seconds, to allow for the updates to arrive
        timeout=1.0,
        # seconds, to allow for network delays and processing time
        network_delay=network_delay if network_delay is not None else 0.1,
    )
