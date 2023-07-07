from .serviceclientbase import HostedServiceClient
from ...dispatch import DispatchKey
from ...namespaces import PrefixesEnum
from ...xml_types import msg_qnames
from ...xml_types.actions import Actions


class StateEventClient(HostedServiceClient):
    port_type_name = PrefixesEnum.SDC.tag('StateEventService')
    notifications = (DispatchKey(Actions.EpisodicMetricReport, msg_qnames.EpisodicMetricReport),
                     DispatchKey(Actions.EpisodicAlertReport, msg_qnames.EpisodicAlertReport),
                     DispatchKey(Actions.EpisodicComponentReport, msg_qnames.EpisodicComponentReport),
                     DispatchKey(Actions.EpisodicOperationalStateReport, msg_qnames.EpisodicOperationalStateReport),
                     DispatchKey(Actions.PeriodicMetricReport, msg_qnames.PeriodicMetricReport),
                     DispatchKey(Actions.PeriodicAlertReport, msg_qnames.PeriodicAlertReport),
                     DispatchKey(Actions.PeriodicComponentReport, msg_qnames.PeriodicComponentReport),
                     DispatchKey(Actions.PeriodicOperationalStateReport, msg_qnames.PeriodicOperationalStateReport),
                     )
