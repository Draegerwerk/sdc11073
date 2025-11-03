"""Tests GetMdib and GetContextStates."""

from __future__ import annotations

import logging
import typing

from sdc11073.xml_types import actions, pm_qnames

if typing.TYPE_CHECKING:
    from sdc11073.consumer import ContextServiceClient, GetServiceClient, SdcConsumer
    from sdc11073.xml_types import msg_types

__STEP__ = '3'
logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')


def test_3a(consumer: SdcConsumer) -> bool:
    """The Reference Provider answers to GetMdib."""
    step = f'{__STEP__}a'
    get_service: GetServiceClient = consumer.get_service_client

    try:
        result = get_service.get_mdib()
    except Exception:
        logger.exception('Error during %s reference provider answers to a GetMdib', step, extra={'step': step})
        return False
    if result.action == actions.Actions.GetMdibResponse:
        logger.info('The reference provider answered to GetMdib with a GetMdibResponse.', extra={'step': step})
        return True
    logger.error(
        'The reference provider answered to GetMdib with an unexpected action: %s.',
        result.action,
        extra={'step': step},
    )
    return False


def test_3b(consumer: SdcConsumer) -> bool:
    """The Reference Provider answers to GetContextStates with at least one location context state."""
    step = f'{__STEP__}b'
    minimum_location_context_states = 1

    context_service: ContextServiceClient | None = consumer.context_service_client
    if context_service is None:
        logger.error('The reference provider does not offer a context service.', extra={'step': step})
        return False
    result: msg_types.GetContextStatesResponse = typing.cast(
        'msg_types.GetContextStatesResponse',
        context_service.get_context_states().result,
    )
    loc_states = [s for s in result.ContextState if pm_qnames.LocationContextState == s.NODETYPE]
    if len(loc_states) >= minimum_location_context_states:
        logger.info(
            'The reference provider provides %d location context state(s), which is more than the minimum of %d.',
            len(loc_states),
            minimum_location_context_states,
            extra={'step': step},
        )
        return True
    logger.error(
        'The reference provider provides %d location context state(s), which is less than the minimum of %d.',
        len(loc_states),
        minimum_location_context_states,
        extra={'step': step},
    )
    return False
