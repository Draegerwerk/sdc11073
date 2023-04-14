from .serviceclientbase import HostedServiceClient


class DescriptionEventClient(HostedServiceClient):
    subscribeable_actions = ('DescriptionModificationReport',)
