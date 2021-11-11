from .serviceclientbase import HostedServiceClient


class WaveformClient(HostedServiceClient):
    subscribeable_actions = ('Waveform',)
