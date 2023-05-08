from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lxml import etree as etree_

from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from .subscriptionmgr_base import SubscriptionBase, SubscriptionsManagerBase
from .. import observableproperties
from ..httpserver.compression import CompressionHandler
from ..pysoap.soapclient import HTTPReturnCodeError
from ..xml_types import eventing_types as evt_types
from ..xml_types.dpws_types import DeviceEventingFilterDialectURI

if TYPE_CHECKING:
    from ..dispatch import RequestData


class ActionBasedSubscription(SubscriptionBase):
    """Subscription for specific actions.
    Actions are a space separated list of strings in FilterType.text"""
    supported_filter_dialect = DeviceEventingFilterDialectURI.ACTION

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actions_filter: list[str] = []

        if self.filter_type is not None:
            self.actions_filter.extend(self.filter_type.text.split())

    def matches(self, what: Any):
        """

        :param what: this must be a string
        :return:
        """
        action: str = what.strip()  # just to be sure there are no spaces
        for filter_string in self.actions_filter:
            if filter_string.endswith(action):
                return True
        return False

    def short_filter_names(self) -> list[str]:
        return [f.split('/')[-1] for f in self.actions_filter]

    def __repr__(self):
        try:
            if self.notify_ref_params is None:
                ref_ident = '<none>'
            else:
                ref_ident = str(
                    self.notify_ref_params)
        except TypeError:
            ref_ident = '<unknown>'
        return f'{self.__class__.__name__}(notify_to={self.notify_to_address} ident={ref_ident}, ' \
               f'my identifier={self.identifier_uuid.hex}, expires={self.remaining_seconds}, ' \
               f'filter={self.short_filter_names()})'


class BicepsSubscription(ActionBasedSubscription):

    def send_notification_report(self, body_node: etree_.Element, action: str):
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


class SubscriptionsManagerPath(ActionBasedSubscriptionsManager):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParam(ActionBasedSubscriptionsManager):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
