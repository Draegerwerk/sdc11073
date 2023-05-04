from .serviceclientbase import HostedServiceClient
from ...xml_types.actions import Actions
from ...xml_types import msg_qnames
from ...dispatch import DispatchKey


class DescriptionEventClient(HostedServiceClient):
    notifications = (DispatchKey(Actions.DescriptionModificationReport, msg_qnames.DescriptionModificationReport),)
