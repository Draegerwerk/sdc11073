from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import msg_qnames
from sdc11073.xml_types.actions import Actions

from .serviceclientbase import HostedServiceClient


class WaveformClient(HostedServiceClient):
    """Client for WaveformService."""

    port_type_name = PrefixesEnum.SDC.tag('WaveformService')
    notifications = (DispatchKey(Actions.Waveform, msg_qnames.WaveformStream),)
