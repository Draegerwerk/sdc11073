"""Tests BICEPS Services Discovery and binding."""

import logging

from sdc11073.consumer import SdcConsumer
from pat.ReferenceTestV2.consumer import result_collector

__STEP__ = '2'
logger = logging.getLogger(f'pat.consumer.step_{__STEP__}')

def step_2a(consumer: SdcConsumer):
    """The Reference Provider answers to TransferGet."""  # noqa: D401
    step = f'{__STEP__}a'

    # because consumer is not subscribed, the soap client needs to be manually started and stopped
    try:
        consumer.get_soap_client(consumer._device_location).connect()
        _ = consumer._get_metadata()
        consumer.get_soap_client(consumer._device_location).close()
    except Exception as ex:
        logger.exception('Error during %s reference provider answers to a TransferGet', step)
        result_collector.ResultCollector.log_failure(step=step, message=f'Error getting metadata: {ex}')


def step_2b(consumer: SdcConsumer):
    """The Reference Consumer renews at least one subscription once during the test phase; the Reference Provider grants subscriptions of at most 15 seconds (this allows for the Reference Consumer to verify if auto-renew works)."""  # noqa: D401, E501, W505
    