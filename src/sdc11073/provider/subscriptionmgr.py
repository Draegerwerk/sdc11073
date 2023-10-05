from __future__ import annotations

from typing import TYPE_CHECKING


from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from .subscriptionmgr_base import ActionBasedSubscription, SubscriptionsManagerBase
from sdc11073 import observableproperties
from sdc11073.httpserver.compression import CompressionHandler
from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.xml_types import eventing_types as evt_types
from sdc11073.xml_types.dpws_types import DeviceEventingFilterDialectURI

if TYPE_CHECKING:
    from lxml import etree as etree_
    from sdc11073.dispatch import RequestData
    from sdc11073 import xml_utils


class BicepsSubscription(ActionBasedSubscription):
    """ This extends ActionBasedSubscription with the ability to send notifications.
    The class is used by ActionBasedSubscriptionsManager."""
    def send_notification_report(self, body_node: xml_utils.LxmlElement, action: str):
        if not self.is_valid:
            return
        inf = HeaderInformationBlock(addr_to=self.notify_to_address,
                                     action=action,
                                     addr_from=None,
                                     reference_parameters=self.notify_ref_params)
        message = self._mk_notification_message(inf, body_node)
        try:
            soap_client = self._get_soap_client()
            roundtrip_timer = observableproperties.SingleValueCollector(soap_client, 'roundtrip_time')

            soap_client.post_message_to(self.notify_to_url.path, message,
                                        msg=f'send_notification_report {action}')
            try:
                roundtrip_time = roundtrip_timer.result(0)
                self.last_roundtrip_times.append(roundtrip_time)
                self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
            except observableproperties.CollectTimeoutError:
                pass
            self.notify_errors = 0
            self._is_connection_error = False
        except HTTPReturnCodeError:
            self.notify_errors += 1
            raise
        except Exception:  # any other exception is handled as an unreachable location (disconnected)
            self.notify_errors += 1
            self._is_connection_error = True
            raise


class ActionBasedSubscriptionsManager(SubscriptionsManagerBase):
    """This is the synchronous version of the subscription manager for all BICEPS subscriptions."""
    supported_filter_dialect = DeviceEventingFilterDialectURI.ACTION
    subscription_cls = BicepsSubscription

    def _mk_subscription_instance(self, request_data: RequestData) -> ActionBasedSubscription:
        subscribe_request = evt_types.Subscribe.from_node(request_data.message_data.p_msg.msg_node)
        filter_type = subscribe_request.Filter
        if filter_type is None:
            raise ValueError(f'No filter provided for {self.__class__.__name__}')
        if filter_type.Dialect != self.supported_filter_dialect:
            raise ValueError(
                f'Invalid filter dialect, got {filter_type.Dialect}, expect {self.supported_filter_dialect}')

        accepted_encodings = CompressionHandler.parse_header(request_data.http_header.get('Accept-Encoding'))
        return self.subscription_cls(self, subscribe_request, accepted_encodings, self.base_urls,
                                     self._max_subscription_duration, self._soap_client_pool,
                                     msg_factory=self._msg_factory, log_prefix=self._logger.log_prefix)


class PathDispatchingSubscriptionsManager(ActionBasedSubscriptionsManager):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class ReferenceParamSubscriptionsManager(ActionBasedSubscriptionsManager):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
