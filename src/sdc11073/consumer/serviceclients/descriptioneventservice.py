from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import msg_qnames
from sdc11073.xml_types.actions import Actions

from .serviceclientbase import HostedServiceClient


class DescriptionEventClient(HostedServiceClient):
    """Client for DescriptionEventService."""

    port_type_name = PrefixesEnum.SDC.tag('DescriptionEventService')
    notifications = (DispatchKey(Actions.DescriptionModificationReport, msg_qnames.DescriptionModificationReport),)
