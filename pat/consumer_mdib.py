"""Consumer mdib with extra methods for reference tests."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from tutorial.codedvaluecomparator import _coded_value_comparator

from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods

if TYPE_CHECKING:
    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgreader import ReceivedMessage


class ConsumerMdibMethodsReferenceTest(ConsumerMdibMethods):
    """Consumer mdib reference test."""

    def __init__(self, consumer_mdib: ConsumerMdib, logger: LoggerAdapter):
        super().__init__(consumer_mdib, logger)
        self.alert_condition_type_concept_updates: list[float] = []  # for test 5a.1
        self._last_alert_condition_type_concept_updates = time.monotonic()  # timestamp
        self.DETERMINATIONTIME_WARN_LIMIT = 2.0

        self.alert_condition_cause_remedy_updates: list[float] = []  # for test 5a.2
        self._last_alert_condition_cause_remedy_updates = time.monotonic()  # timestamp

        self.unit_of_measure_updates: list[float] = []  # for test 5a.3
        self._last_unit_of_measure_updates = time.monotonic()  # timestamp

    def _on_description_modification_report(self, received_message_data: ReceivedMessage):  # noqa: C901, PLR0912
        """For Test 5a.1 check if the concept description of updated alert condition Type changed.

        For Test 5a.2 check if alert condition cause-remedy information changed.
        """
        # only do this when mdib is completely initialized, otherwise access to old descriptors fails
        if self._mdib.is_initialized:
            cls = self._mdib.data_model.msg_types.DescriptionModificationReport
            report = cls.from_node(received_message_data.p_msg.msg_node)
            now = time.monotonic()
            dmt = self._mdib.sdc_definitions.data_model.msg_types.DescriptionModificationType
            for report_part in report.ReportPart:
                modification_type = report_part.ModificationType
                if modification_type == dmt.UPDATE:
                    for descriptor_container in report_part.Descriptor:
                        if descriptor_container.is_alert_condition_descriptor:
                            old_descriptor = self._mdib.descriptions.handle.get_one(descriptor_container.Handle)
                            # test 5a.1
                            if descriptor_container.Type.ConceptDescription != old_descriptor.Type.ConceptDescription:
                                print(
                                    f'concept description {descriptor_container.Type.ConceptDescription} <=> '
                                    f'{old_descriptor.Type.ConceptDescription}',
                                )
                                self.alert_condition_type_concept_updates.append(
                                    now - self._last_alert_condition_type_concept_updates,
                                )
                                self._last_alert_condition_type_concept_updates = now
                            # test 5a.2
                            # (CauseInfo is a list)
                            detected_5a2 = False
                            if len(descriptor_container.CauseInfo) != len(old_descriptor.CauseInfo):
                                print(
                                    f'RemedyInfo no. of CauseInfo {len(descriptor_container.CauseInfo)} <=> '
                                    f'{len(old_descriptor.CauseInfo)}',
                                )
                                detected_5a2 = True
                            else:
                                for i, cause_info in enumerate(descriptor_container.CauseInfo):
                                    old_cause_info = old_descriptor.CauseInfo[i]
                                    if cause_info.RemedyInfo != old_cause_info.RemedyInfo:
                                        print(f'RemedyInfo {cause_info.RemedyInfo} <=> {old_cause_info.RemedyInfo}')
                                        detected_5a2 = True
                            if detected_5a2:
                                self.alert_condition_cause_remedy_updates.append(
                                    now - self._last_alert_condition_cause_remedy_updates,
                                )
                                self._last_alert_condition_cause_remedy_updates = now
                        elif descriptor_container.is_metric_descriptor:
                            # test 5a.3
                            old_descriptor = self._mdib.descriptions.handle.get_one(descriptor_container.Handle)
                            if not _coded_value_comparator(old_descriptor.Unit, descriptor_container.Unit):
                                self.unit_of_measure_updates.append(now - self._last_unit_of_measure_updates)
                                self._last_unit_of_measure_updates = now

        else:
            # reset timestamps to avoid large delta times on first update after initialization
            self._last_alert_condition_type_concept_updates = time.monotonic()
            self._last_alert_condition_cause_remedy_updates = time.monotonic()
            self._last_unit_of_measure_updates = time.monotonic()
        # call base class implementation to update mdib / buffer reports
        super()._on_description_modification_report(received_message_data)


def init_mdib(consumer: SdcConsumer) -> ConsumerMdib:
    """Initialize the consumer mdib with the extra consumer mdib methods relevant for pat tests."""
    mdib = ConsumerMdib(consumer, extras_cls=ConsumerMdibMethodsReferenceTest)
    mdib.init_mdib()
    return mdib
