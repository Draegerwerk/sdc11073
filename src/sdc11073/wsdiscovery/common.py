import logging

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.pysoap.msgfactory import MessageFactory
from sdc11073.pysoap.msgreader import MessageReader

message_factory = MessageFactory(SdcV1Definitions, None, logger=logging.getLogger('sdc.discover.msg'))
message_reader = MessageReader(SdcV1Definitions, None, logger=logging.getLogger('sdc.discover.msg'))

MULTICAST_PORT = 3702
MULTICAST_IPV4_ADDRESS = "239.255.255.250"
MULTICAST_OUT_TTL = 15  # Time To Live for multicast_out
