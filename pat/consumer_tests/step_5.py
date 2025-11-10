"""Tests alert conditions."""

import functools
import logging
import threading
import time
import typing
from collections.abc import Iterator

from sdc11073.mdib import ConsumerMdib, descriptorcontainers
from sdc11073.observableproperties import observables

__STEP__ = '5'

from sdc11073.xml_types import msg_types

logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def test_5a(mdib: ConsumerMdib) -> bool:
    """The Reference Provider produces at least 1 update every 10 seconds comprising

    - Update alert condition concept description of Type: change at least the content of the first localized text of one alert condition
    - Update alert condition cause-remedy information: change at least the content of the first localized text of either cause or remedy texts of one alert condition
    - Update Unit of measure (metrics): change at least the code of the unit of measure of one metric
    """  # noqa: D400, D415, E501, W505
    step = f'{__STEP__}a'
    max_time_between_updates = 10
    expected_updates = 2
    # Wait at least twice the specified interval (plus a one-second margin)
    # so that at least two description updates of each specified type have to occur.
    timeout = expected_updates * max_time_between_updates + 1
    time.sleep(timeout)
    test_results: list[bool] = []

    updates: list[float] = mdib.xtra.alert_condition_type_concept_updates

    if len(updates) < expected_updates:
        logger.error('Less than %d updates of Alert Condition Concept Description were recorded within %d seconds. '
                     'Number of received updates: %d',
                     expected_updates,
                     timeout,
                     len(updates),
                     extra={'step': step})
        test_results.append(False)
    elif max(updates) <= max_time_between_updates:
        logger.info(
            'Alert condition concept description updates are at least %d seconds apart',
            max_time_between_updates,
            extra={'step': step},
        )
        test_results.append(True)
    else:
        logger.error(
            'Alert condition concept description updates are more than %d seconds apart: %s',
            max_time_between_updates,
            [update for update in updates if update > max_time_between_updates],
            extra={'step': step},
        )
        test_results.append(False)

    updates = mdib.xtra.alert_condition_cause_remedy_updates
    if len(updates) < expected_updates:
        logger.error('Less than %d updates of Alert Condition cause-remedy information were recorded within %d seconds. '
                     'Number of received updates: %d',
                     expected_updates,
                     timeout,
                     len(updates),
                     extra={'step': step})
        test_results.append(False)
    elif max(updates) <= max_time_between_updates:
        logger.info(
            'Alert condition cause-remedy information updates are at least %d seconds apart',
            max_time_between_updates,
            extra={'step': step},
        )
        test_results.append(True)
    else:
        logger.error(
            'Alert condition cause-remedy information updates are more than %d seconds apart: %s',
            max_time_between_updates,
            [update for update in updates if update > max_time_between_updates],
            extra={'step': step},
        )
        test_results.append(False)

    updates = mdib.xtra.unit_of_measure_updates
    if len(updates) < expected_updates:
        logger.error('Less than %d updates of unit of measures were recorded within %d seconds. '
                     'Number of received updates: %d',
                     expected_updates,
                     timeout,
                     len(updates),
                     extra={'step': step})
        test_results.append(False)
    elif max(updates) <= max_time_between_updates:
        logger.info(
            'Unit of measure updates are at least %d seconds apart',
            max_time_between_updates,
            extra={'step': step},
        )
        test_results.append(True)
    else:
        logger.error(
            'Unit of measure updates are more than %d seconds apart: %s',
            max_time_between_updates,
            [update for update in updates if update > max_time_between_updates],
            extra={'step': step},
        )
        test_results.append(False)

    return any(test_results) and all(test_results)


T = typing.TypeVar('T')


def _iter_dmr_by_type(
    dmr: msg_types.DescriptionModificationReport,
    clazz: type[T],
    modification_type: msg_types.DescriptionModificationType,
) -> Iterator[T]:
    for report_part in dmr.ReportPart:
        report_part: msg_types.DescriptionModificationReportPart
        if report_part.ModificationType == modification_type:
            for descriptor in report_part.Descriptor:
                if isinstance(descriptor, clazz):
                    yield descriptor


def _on_description_modification_report(
    step: str,
    created_vmds: set[str],
    vmd_created: threading.Event,
    vmd_deleted: threading.Event,
    dmr: msg_types.DescriptionModificationReport,
):
    for vmd_descriptor in _iter_dmr_by_type(
        dmr,
        descriptorcontainers.VmdDescriptorContainer,
        msg_types.DescriptionModificationType.CREATE,
    ):
        logger.debug('Found created VmdDescriptor with the handle %s.', vmd_descriptor.Handle, extra={'step': step})
        found_channel_descriptor = None
        for channel_descriptor in _iter_dmr_by_type(
            dmr,
            descriptorcontainers.ChannelDescriptorContainer,
            msg_types.DescriptionModificationType.CREATE,
        ):
            if channel_descriptor.parent_handle == vmd_descriptor.Handle:
                logger.debug(
                    'Found ChannelDescriptor with handle %s in created VMD %s.',
                    channel_descriptor.Handle,
                    vmd_descriptor.Handle,
                    extra={'step': step},
                )
                found_channel_descriptor = channel_descriptor
                break
        if not found_channel_descriptor:  # update contains no channel descriptor, ignore create vmd
            continue
        for metric_descriptor in _iter_dmr_by_type(
            dmr,
            descriptorcontainers.AbstractMetricDescriptorContainer,
            msg_types.DescriptionModificationType.CREATE,
        ):
            if metric_descriptor.parent_handle == found_channel_descriptor.Handle:
                logger.debug(
                    'Found MetricDescriptor with handle %s in created ChannelDescriptor %s.',
                    metric_descriptor.Handle,
                    found_channel_descriptor.Handle,
                    extra={'step': step},
                )
                created_vmds.add(vmd_descriptor.Handle)
                vmd_created.set()
                break

    if not vmd_created.is_set():
        return  # only continue if a VMD with Channel and Metric has already been created
    for vmd_descriptor in _iter_dmr_by_type(
        dmr,
        descriptorcontainers.VmdDescriptorContainer,
        msg_types.DescriptionModificationType.DELETE,
    ):
        if vmd_descriptor.Handle in created_vmds:
            logger.info(
                'VMD with the handle %s deleted after creation.',
                vmd_descriptor.Handle,
                extra={'step': step},
            )
            vmd_deleted.set()
            break


def test_5b(mdib: ConsumerMdib) -> bool:
    """The Reference Provider produces at least 1 insertion followed by a deletion every 10 seconds comprising

    - Insert a VMD including Channels including metrics (inserted VMDs/Channels/Metrics are required to have a new handle assigned on each insertion such that containment tree entries are not recycled). (Tests for the handling of re-insertion of previously inserted objects should be tested additionally)
    - Remove the VMD
    """  # noqa: D400, D415, E501, W505
    step = f'{__STEP__}b'
    timeout = 10.0
    vmd_created = threading.Event()
    vmd_deleted = threading.Event()

    observer = functools.partial(
        _on_description_modification_report,
        step,
        set(),
        vmd_created,
        vmd_deleted,
    )
    with observables.bound_context(mdib, description_modifications=observer):
        if not vmd_created.wait(timeout):
            logger.error(
                'No VMD was created within the timeout period of %s seconds',
                timeout,
                extra={'step': step},
            )
            return False
        if vmd_deleted.wait(timeout):
            logger.info(
                'VMD was created and deleted within the timeout period of %s seconds',
                timeout,
                extra={'step': step},
            )
            return True
        logger.error(
            'No VMD was created and deleted within the timeout period of %s seconds',
            timeout,
            extra={'step': step},
        )
        return False
