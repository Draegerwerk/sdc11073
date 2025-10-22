"""Tests BICEPS Services Discovery and binding."""

import logging
import time

from sdc11073.consumer import ConsumerSubscription, SdcConsumer

__STEP__ = '2'


logger = logging.getLogger('pat.consumer')


def test_2a(consumer: SdcConsumer) -> bool:
    """The Reference Provider answers to TransferGet."""
    step = f'{__STEP__}a'

    # because consumer is not subscribed, the soap client needs to be manually started and stopped
    try:
        consumer.get_soap_client(consumer._device_location).connect()  # noqa: SLF001
        _ = consumer._get_metadata()  # noqa: SLF001
    except Exception:
        logger.exception('Error during TransferGet', extra={'step': step})
        return False
    finally:
        consumer.get_soap_client(consumer._device_location).close()  # noqa: SLF001

    logger.info('Reference provider answers to a TransferGet.', extra={'step': step})
    return True


def test_2b(consumer: SdcConsumer) -> bool:
    """The Reference Consumer renews at least one subscription once during the test phase; the Reference Provider grants subscriptions of at most 15 seconds (this allows for the Reference Consumer to verify if auto-renew works)."""  # noqa: E501, W505
    step = f'{__STEP__}b'
    max_subscription_duration = 15
    consumer.start_all()  # makes a subscription and requests 60 seconds per default in do_subscribe
    for filter_text, subscription in consumer.subscription_mgr.subscriptions.items():
        filter_text: str
        subscription: ConsumerSubscription
        if subscription.granted_expires <= max_subscription_duration:
            logger.info(
                'Subscription duration granted "%s" is at most 15 seconds for subscription with the filter "%s".',
                subscription.granted_expires,
                filter_text,
                extra={'step': step},
            )
        else:
            logger.error(
                'Subscription duration granted "%s" is more than 15 seconds for subscription with the filter "%s".',
                subscription.granted_expires,
                filter_text,
                extra={'step': step},
            )
            # no need to continue if one subscription failed, otherwise test can wait indefinitely
            #   if granted expiration is very long
            return False
    subscriptions: list[ConsumerSubscription] = list(consumer.subscription_mgr.subscriptions.values())
    subscriptions.sort(key=lambda s: s.granted_expires)
    timeout = subscriptions[0].granted_expires + 1
    logger.info('Sleeping %d seconds to allow auto-renew of at least one subscription.', timeout, extra={'step': step})
    time.sleep(timeout)
    if subscriptions[0].is_subscribed:
        logger.info('At least one subscription was auto-renewed successfully.', extra={'step': step})
        return True
    logger.error('No subscription was auto-renewed successfully.', extra={'step': step})
    return False
