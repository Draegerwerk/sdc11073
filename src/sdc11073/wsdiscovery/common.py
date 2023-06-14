from __future__ import annotations

import logging

from ..definitions_sdc import SDC_v1_Definitions
from ..pysoap.msgfactory import MessageFactory
from ..pysoap.msgreader import MessageReader

message_factory = MessageFactory(SDC_v1_Definitions, None, logger=logging.getLogger('sdc.discover.msg'))
message_reader = MessageReader(SDC_v1_Definitions, None, logger=logging.getLogger('sdc.discover.msg'))

MULTICAST_PORT = 3702
MULTICAST_IPV4_ADDRESS = "239.255.255.250"
MULTICAST_OUT_TTL = 15  # Time To Live for multicast_out
