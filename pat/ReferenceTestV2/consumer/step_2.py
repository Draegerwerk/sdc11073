"""Tests BICEPS Services Discovery and binding."""

import logging

from pat.ReferenceTestV2.consumer import result_collector
from sdc11073.consumer import ConsumerSubscription, SdcConsumer

__STEP__ = '2'
logger = logging.getLogger('pat.consumer')


def test_2a(consumer: SdcConsumer):
    """The Reference Provider answers to TransferGet."""
    step = f'{__STEP__}a'

    # because consumer is not subscribed, the soap client needs to be manually started and stopped
    try:
        consumer.get_soap_client(consumer._device_location).connect()  # noqa: SLF001
        _ = consumer._get_metadata()  # noqa: SLF001
        consumer.get_soap_client(consumer._device_location).close()  # noqa: SLF001
    except Exception as ex:
        logger.exception('The reference provider answers to a TransferGet', extra={'step': step})
        result_collector.ResultCollector.log_failure(
            step=step,
            message=f'Error during {step} reference provider answers to a TransferGet: {ex}',
        )


def test_2b(consumer: SdcConsumer):
    """The Reference Consumer renews at least one subscription once during the test phase; the Reference Provider grants subscriptions of at most 15 seconds (this allows for the Reference Consumer to verify if auto-renew works)."""  # noqa: E501, W505
    step = f'{__STEP__}b'
    max_subscription_duration = 15
    consumer.start_all()  # makes a subscription and requests 60 seconds per default in do_subscribe
    for filter_text, subscription in consumer.subscription_mgr.subscriptions.items():
        filter_text: str
        subscription: ConsumerSubscription
        if subscription.granted_expires <= max_subscription_duration:
            result_collector.ResultCollector.log_success(
                step=step,
                message=f'Subscription duration granted "{subscription.granted_expires}" is at most 15 seconds for '
                f'subscription with the filter "{filter_text}".',
            )
        else:
            result_collector.ResultCollector.log_failure(
                step=step,
                message=f'Subscription duration granted "{subscription.granted_expires}" is more than 15 seconds for '
                f'subscription with the filter "{filter_text}".',
            )
