from .serviceclientbase import HostedServiceClient


class StateEventClient(HostedServiceClient):
    subscribeable_actions = ('EpisodicMetricReport',
                             'EpisodicAlertReport',
                             'EpisodicComponentReport',
                             'EpisodicOperationalStateReport',
                             'PeriodicMetricReport',
                             'PeriodicAlertReport',
                             'PeriodicComponentReport',
                             'PeriodicOperationalStateReport'
                             )
