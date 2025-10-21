"""Tests GetMdib and GetContextStates."""

import logging
import typing

from pat.consumer import result_collector
from sdc11073.consumer import ContextServiceClient, GetServiceClient, SdcConsumer
from sdc11073.xml_types import actions, msg_types, pm_qnames

__STEP__ = '3'
logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def test_3a(consumer: SdcConsumer):
    """The Reference Provider answers to GetMdib."""
    step = f'{__STEP__}a'
    get_service: GetServiceClient = consumer.get_service_client

    try:
        result = get_service.get_mdib()
        if result.action == actions.Actions.GetMdibResponse:
            result_collector.ResultCollector.log_success(
                step=step,
                message='The reference provider answered to GetMdib with a GetMdibResponse.',
            )
        else:
            result_collector.ResultCollector.log_failure(
                step=step,
                message=f'The reference provider answered to GetMdib with an unexpected action: {result.action}.',
            )
    except Exception as ex:
        logger.exception('Error during %s reference provider answers to a GetMdib', step, extra={'step': step})
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Error during {step} reference provider answers to a GetMdib: {ex}',
        )


def test_3b(consumer: SdcConsumer):
    """The Reference Provider answers to GetContextStates with at least one location context state."""
    step = f'{__STEP__}b'
    minimum_location_context_states = 1

    context_service: ContextServiceClient | None = consumer.context_service_client
    if context_service is None:
        result_collector.ResultCollector.log_failure(
            step=step,
            message='The reference provider does not offer a context service.',
        )
        return
    result: msg_types.GetContextStatesResponse = typing.cast(
        'msg_types.GetContextStatesResponse',
        context_service.get_context_states().result,
    )
    loc_states = [s for s in result.ContextState if pm_qnames.LocationContextState == s.NODETYPE]
    if len(loc_states) >= minimum_location_context_states:
        result_collector.ResultCollector.log_success(
            step=step,
            message=f'The reference provider provides {len(loc_states)} location context state(s), which is more than '
            f'the minimum of {minimum_location_context_states}.',
        )
    else:
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'The reference provider provides {len(loc_states)} location context state(s), which is more less '
            f'the minimum of {minimum_location_context_states}.',
        )
