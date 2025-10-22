"""Implementation of reference consumer v2."""

from __future__ import annotations

import logging
import pathlib
import sys
import time
from concurrent import futures
from typing import TYPE_CHECKING

from pat import common
from pat.consumer_tests import step_1, step_2, step_3, step_4, step_5, step_6
from sdc11073.consumer import SdcConsumer
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods
from sdc11073.wsdiscovery import WSDiscovery

if TYPE_CHECKING:
    import sdc11073.certloader
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgreader import ReceivedMessage
    from sdc11073.wsdiscovery.service import Service

ConsumerMdibMethods.DETERMINATIONTIME_WARN_LIMIT = 2.0


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


def run_ref_test(
    adapter: str,
    epr: str,
    ssl_context_container: sdc11073.certloader.SSLContextContainer | None,
    execute_1a: bool,
    network_delay: float,
) -> bool:
    """Run reference test."""
    wsd = WSDiscovery(adapter)
    wsd.start()
    res_1a = step_1.test_1a(wsd, epr) if execute_1a else None
    res_1b = step_1.test_1b(wsd, epr)
    if not res_1b:
        return False
    services: list[Service] = wsd.search_services(timeout=-1)  # services have already been found in 1b
    service = next(s for s in services if s.epr == epr)
    consumer = SdcConsumer.from_wsd_service(service, ssl_context_container=ssl_context_container, validate=True)
    res_2a = step_2.test_2a(consumer)
    res_2b = step_2.test_2b(consumer)
    if not res_2b:
        return False

    res_3a = step_3.test_3a(consumer)
    res_3b = step_3.test_3b(consumer)

    mdib = ConsumerMdib(consumer, extras_cls=ConsumerMdibMethodsReferenceTest)
    mdib.init_mdib()

    with futures.ThreadPoolExecutor() as pool:
        thread_test_4a = pool.submit(step_4.test_4a, mdib)
        thread_test_4b = pool.submit(step_4.test_4b, mdib)
        thread_test_4c = pool.submit(step_4.test_4c, mdib)
        thread_test_4d = pool.submit(step_4.test_4d, mdib)
        thread_test_4e = pool.submit(step_4.test_4e, mdib)
        thread_test_4f = pool.submit(step_4.test_4f, mdib, network_delay)
        thread_test_4g = pool.submit(step_4.test_4g, mdib)
        thread_test_4h = pool.submit(step_4.test_4h, mdib)
        thread_test_4i = pool.submit(step_4.test_4i, mdib, network_delay)
        thread_test_5a = pool.submit(step_5.test_5a, mdib)
        thread_test_5b = pool.submit(step_5.test_5b, mdib)
        thread_test_6b = pool.submit(step_6.test_6b, consumer)
        thread_test_6c = pool.submit(step_6.test_6c, consumer)
        thread_test_6d = pool.submit(step_6.test_6d, consumer)
        thread_test_6e = pool.submit(step_6.test_6e, consumer)
        thread_test_6f = pool.submit(step_6.test_6f, consumer)

        test_4a = thread_test_4a.result()
        test_4b = thread_test_4b.result()
        test_4c = thread_test_4c.result()
        test_4d = thread_test_4d.result()
        test_4e = thread_test_4e.result()
        test_4f = thread_test_4f.result()
        test_4g = thread_test_4g.result()
        test_4h = thread_test_4h.result()
        test_4i = thread_test_4i.result()
        test_5a = thread_test_5a.result()
        test_5b = thread_test_5b.result()
        test_6b = thread_test_6b.result()
        test_6c = thread_test_6c.result()
        test_6d = thread_test_6d.result()
        test_6e = thread_test_6e.result()
        test_6f = thread_test_6f.result()
    if execute_1a:
        print('1a:', res_1a)
    print('1b:', res_1b)
    print('2a:', res_2a)
    print('2b:', res_2b)
    print('3a:', res_3a)
    print('3b:', res_3b)
    print('4a:', test_4a)
    print('4b:', test_4b)
    print('4c:', test_4c)
    print('4d:', test_4d)
    print('4e:', test_4e)
    print('4f:', test_4f)
    print('4g:', test_4g)
    print('4h:', test_4h)
    print('4i:', test_4i)
    print('5a:', test_5a)
    print('5b:', test_5b)
    print('6b:', test_6b)
    print('6c:', test_6c)
    print('6d:', test_6d)
    print('6e:', test_6e)
    print('6f:', test_6f)

    results = [
        res_1b,
        res_2a,
        res_2b,
        res_3a,
        res_3b,
        test_4a,
        test_4b,
        test_4c,
        test_4d,
        test_4e,
        test_4f,
        test_4g,
        test_4h,
        test_4i,
        test_5a,
        test_5b,
        test_6b,
        test_6c,
        test_6d,
        test_6e,
        test_6f,
    ]
    if execute_1a:
        results.append(res_1a)
    return any(results) and all(results)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='run plug-a-thon test consumer')
    parser.add_argument('--adapter', required=True, help='Network adapter IP address to use.')
    parser.add_argument('--epr', required=True, help='Explicit endpoint reference to search for.')
    parser.add_argument('--certificate-folder', type=pathlib.Path, help='Folder containing TLS artifacts.')
    parser.add_argument('--ssl-password', help='Password for encrypted TLS private key.')
    parser.add_argument('--network-delay', type=float, help='Network delay to use in seconds.', default=0.1)
    parser.add_argument('--execute-1a', action='store_true', help='Execute test step 1a.')

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(step)s - %(message)s'))
    logger = logging.getLogger('pat.consumer')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    args = parser.parse_args()
    passed = run_ref_test(
        adapter=args.adapter,
        epr=args.epr,
        ssl_context_container=common.get_ssl_context(args.certificate_folder, args.ssl_password)
        if args.certificate_folder
        else None,
        network_delay=args.network_delay,
        execute_1a=args.execute_1a,
    )
    sys.exit(0 if passed else 1)
