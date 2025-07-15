"""Tests alert conditions."""

import functools
import logging
import threading
import time

from pat.ReferenceTestV2.consumer import result_collector
from sdc11073.mdib import ConsumerMdib
from sdc11073.observableproperties import observables

__STEP__ = '5'

from sdc11073.xml_types import msg_types, pm_qnames

logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def test_5a(mdib: ConsumerMdib):
    """The Reference Provider produces at least 1 update every 10 seconds comprising

    - Update alert condition concept description of Type: change at least the content of the first localized text of one alert condition
    - Update alert condition cause-remedy information: change at least the content of the first localized text of either cause or remedy texts of one alert condition
    - Update Unit of measure (metrics): change at least the code of the unit of measure of one metric
    """  # noqa: D400, D415, E501, W505
    step = f'{__STEP__}a'
    max_time_between_updates = 10

    time.sleep(max_time_between_updates)

    updates: list[float] = mdib.xtra.alert_condition_type_concept_updates
    if max(updates) <= max_time_between_updates:
        result_collector.ResultCollector.log_success(
            step=step,
            message=f'Alert condition concept description updates are at least {max_time_between_updates} '
            f'seconds apart',
        )
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Alert condition concept description updates are more than {max_time_between_updates} '
            f'seconds apart: {[update for update in updates if update > max_time_between_updates]}',
        )

    updates: list[float] = mdib.xtra.alert_condition_cause_remedy_updates
    if max(updates) <= max_time_between_updates:
        result_collector.ResultCollector.log_success(
            step=step,
            message=f'Alert condition cause-remedy information updates are at least {max_time_between_updates} '
            f'seconds apart',
        )
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Alert condition cause-remedy information updates are more than {max_time_between_updates} '
            f'seconds apart: {[update for update in updates if update > max_time_between_updates]}',
        )

    updates: list[float] = mdib.xtra.unit_of_measure_updates
    if max(updates) <= max_time_between_updates:
        result_collector.ResultCollector.log_success(
            step=step,
            message=f'Unit of measure updates are at least {max_time_between_updates} seconds apart',
        )
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Unit of measure updates are more than {max_time_between_updates} seconds apart: '
            f'{[update for update in updates if update > max_time_between_updates]}',
        )


def _on_description_modification_report(
    step: str,
    created_vmds: set[str],
    start_waiting_event: threading.Event,
    delete_after_create_event: threading.Event,
    dmr: msg_types.DescriptionModificationReport,
):
    for report_part in dmr.ReportPart:
        report_part: msg_types.DescriptionModificationReportPart
        logger.debug(
            'received description modification report part %s with descriptor handles %s',
            report_part.ModificationType,
            [descriptor.Handle for descriptor in report_part.Descriptor],
            extra={'step': step},
        )
        for descriptor in report_part.Descriptor:
            if pm_qnames.VmdDescriptor != descriptor.NODETYPE:
                continue
            if report_part.ModificationType == msg_types.DescriptionModificationType.CREATE:
                if descriptor.Handle in created_vmds:
                    logger.error(
                        'Descriptor already created with the handle "%s"',
                        descriptor.Handle,
                        extra={'step': step},
                    )
                else:
                    created_vmds.add(descriptor.Handle)
                    start_waiting_event.set()
            if report_part.ModificationType == msg_types.DescriptionModificationType.DELETE:
                if descriptor.Handle in created_vmds:
                    logger.info(
                        'VMD with the handle %s deleted after creation',
                        descriptor.Handle,
                        extra={'step': step},
                    )
                    delete_after_create_event.set()


def test_5b(mdib: ConsumerMdib):
    """The Reference Provider produces at least 1 insertion followed by a deletion every 10 seconds comprising

    - Insert a VMD including Channels including metrics (inserted VMDs/Channels/Metrics are required to have a new handle assigned on each insertion such that containment tree entries are not recycled). (Tests for the handling of re-insertion of previously inserted objects should be tested additionally)
    - Remove the VMD
    """  # noqa: D400, D415, E501, W505
    step = f'{__STEP__}b'
    timeout = 10.0
    start_waiting_event = threading.Event()
    delete_after_create_event = threading.Event()

    observer = functools.partial(
        _on_description_modification_report,
        step,
        set(),
        start_waiting_event,
        delete_after_create_event,
    )
    with observables.bound_context(mdib, description_modifications=observer):
        if not start_waiting_event.wait(timeout):
            result_collector.ResultCollector.log_failure(
                step=step,
                message=f'No VMD was created within the timeout period of {timeout} seconds',
            )
        if delete_after_create_event.wait(timeout):
            result_collector.ResultCollector.log_success(
                step=step,
                message=f'VMD was created and deleted within the timeout period of {timeout} seconds',
            )
        else:
            result_collector.ResultCollector.log_failure(
                step=step,
                message=f'No VMD was created and deleted within the timeout period of {timeout} seconds',
            )
