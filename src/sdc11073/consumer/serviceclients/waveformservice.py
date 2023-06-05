from .serviceclientbase import HostedServiceClient
from ...xml_types.actions import Actions
from ...xml_types import msg_qnames
from ...dispatch import DispatchKey

class WaveformClient(HostedServiceClient):
    notifications = (DispatchKey(Actions.Waveform, msg_qnames.WaveformStream),)
