from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import msg_qnames
from sdc11073.xml_types.actions import Actions

from .serviceclientbase import HostedServiceClient


class StateEventClient(HostedServiceClient):
    """Client for StateEventService."""

    port_type_name = PrefixesEnum.SDC.tag('StateEventService')
    notifications = (DispatchKey(Actions.EpisodicMetricReport, msg_qnames.EpisodicMetricReport),
                     DispatchKey(Actions.EpisodicAlertReport, msg_qnames.EpisodicAlertReport),
                     DispatchKey(Actions.EpisodicComponentReport, msg_qnames.EpisodicComponentReport),
                     DispatchKey(Actions.EpisodicOperationalStateReport, msg_qnames.EpisodicOperationalStateReport),
                     DispatchKey(Actions.PeriodicMetricReport, msg_qnames.PeriodicMetricReport),
                     DispatchKey(Actions.PeriodicAlertReport, msg_qnames.PeriodicAlertReport),
                     DispatchKey(Actions.PeriodicComponentReport, msg_qnames.PeriodicComponentReport),
                     DispatchKey(Actions.PeriodicOperationalStateReport, msg_qnames.PeriodicOperationalStateReport),
                     DispatchKey(Actions.SystemErrorReport, msg_qnames.SystemErrorReport),
                     )
