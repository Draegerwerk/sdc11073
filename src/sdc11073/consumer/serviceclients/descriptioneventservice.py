from .serviceclientbase import HostedServiceClient
from ...dispatch import DispatchKey
from ...namespaces import PrefixesEnum
from ...xml_types import msg_qnames
from ...xml_types.actions import Actions


class DescriptionEventClient(HostedServiceClient):
    port_type_name = PrefixesEnum.SDC.tag('DescriptionEventService')
    notifications = (DispatchKey(Actions.DescriptionModificationReport, msg_qnames.DescriptionModificationReport),)
