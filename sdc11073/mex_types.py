from .msgtypes import MessageType
from .namespaces import default_ns_helper
########## Meta Data Exchange #########

class GetMetadata(MessageType):
    NODETYPE = default_ns_helper.wsxTag('GetMetadata')
    action = 'http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request'
