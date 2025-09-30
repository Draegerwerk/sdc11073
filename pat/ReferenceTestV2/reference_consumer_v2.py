"""Implementation of reference consumer v2.

If a value is not provided as environment variable, the default value (see code below) will be used.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from concurrent import futures
from typing import TYPE_CHECKING

from pat.ReferenceTestV2 import common
from pat.ReferenceTestV2.consumer import result_collector, step_1, step_2, step_3, step_4, step_5, step_6
from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.consumer import SdcConsumer
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods
from sdc11073.wsdiscovery import WSDiscovery

if TYPE_CHECKING:
    import sdc11073
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgreader import ReceivedMessage
    from sdc11073.wsdiscovery.service import Service

ConsumerMdibMethods.DETERMINATIONTIME_WARN_LIMIT = 2.0


def get_ssl_context() -> sdc11073.certloader.SSLContextContainer | None:
    """Get ssl context from environment or None."""
    if (ca_folder := os.getenv('ref_ca')) is None:  # noqa: SIM112
        return None
    return mk_ssl_contexts_from_folder(
        ca_folder,
        private_key='user_private_key_encrypted.pem',
        certificate='user_certificate_root_signed.pem',
        ca_public_key='root_certificate.pem',
        cyphers_file=None,
        ssl_passwd=os.getenv('ref_ssl_passwd'),  # noqa:SIM112
    )


class ConsumerMdibMethodsReferenceTest(ConsumerMdibMethods):
    """Consumer mdib reference test."""

    def __init__(self, consumer_mdib: ConsumerMdib, logger: LoggerAdapter):
        super().__init__(consumer_mdib, logger)
        self.alert_condition_type_concept_updates: list[float] = []  # for test 5a.1
        self._last_alert_condition_type_concept_updates = time.monotonic()  # timestamp

        self.alert_condition_cause_remedy_updates: list[float] = []  # for test 5a.2
        self._last_alert_condition_cause_remedy_updates = time.monotonic()  # timestamp

        self.unit_of_measure_updates: list[float] = []  # for test 5a.3
        self._last_unit_of_measure_updates = time.monotonic()  # timestamp

    def _on_description_modification_report(self, received_message_data: ReceivedMessage):  # noqa: C901
        """For Test 5a.1 check if the concept description of updated alert condition Type changed.

        For Test 5a.2 check if alert condition cause-remedy information changed.
        """
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
                        if old_descriptor.Unit != descriptor_container.Unit:
                            self.unit_of_measure_updates.append(now - self._last_unit_of_measure_updates)
                            self._last_unit_of_measure_updates = now

        super()._on_description_modification_report(received_message_data)


def connect_client(my_service: Service) -> SdcConsumer:
    """Connect sdc consumer."""
    client = SdcConsumer.from_wsd_service(my_service, ssl_context_container=get_ssl_context(), validate=True)
    client.start_all()
    return client


def run_ref_test():
    """Run reference test."""
    adapter_ip = common.get_network_adapter().ip
    search_epr = common.get_epr()
    # Remark: 1a) is not testable because provider can't be forced to send a hello while this test is running.
    wsd = WSDiscovery(adapter_ip)
    wsd.start()
    if not step_1.test_1b(wsd, search_epr):
        return
    services = wsd.search_services(timeout=-1)
    consumer = SdcConsumer.from_wsd_service(services[0], ssl_context_container=get_ssl_context(), validate=True)
    step_2.test_2a(consumer)
    step_2.test_2b(consumer)

    step_3.test_3a(consumer)
    step_3.test_3b(consumer)

    mdib = ConsumerMdib(consumer, extras_cls=ConsumerMdibMethodsReferenceTest)
    mdib.init_mdib()

    with futures.ThreadPoolExecutor() as pool:
        threads = [
            pool.submit(step_4.test_4a, mdib),
            pool.submit(step_4.test_4b, mdib),
            pool.submit(step_4.test_4c, mdib),
            pool.submit(step_4.test_4d, mdib),
            pool.submit(step_4.test_4e, mdib),
            pool.submit(step_4.test_4f, mdib),
            pool.submit(step_4.test_4g, mdib),
            pool.submit(step_4.test_4h, mdib),
            pool.submit(step_4.test_4i, mdib),
            pool.submit(step_5.test_5a, mdib),
            pool.submit(step_5.test_5b, mdib),
            pool.submit(step_6.test_6b, consumer),
            pool.submit(step_6.test_6c, consumer),
            pool.submit(step_6.test_6d, consumer),
            pool.submit(step_6.test_6e, consumer),
            pool.submit(step_6.test_6f, consumer),
        ]
        for t in threads:
            t.result()


if __name__ == '__main__':
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(step)s - %(message)s'))
    logger = logging.getLogger('pat.consumer')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    run_ref_test()
    result_collector.ResultCollector.print_summary()
    sys.exit(bool(result_collector.ResultCollector.failed))
