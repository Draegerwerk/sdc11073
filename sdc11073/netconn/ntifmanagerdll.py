from socket import AF_INET
import ctypes
from ctypes.wintypes import DWORD, BYTE
import traceback
import struct
import socket
from collections import namedtuple

_iphlpapi = ctypes.windll.iphlpapi

#################################### structures for getAdaptersAddresses #################################

MAX_ADAPTER_ADDRESS_LENGTH = 8
MAX_DHCPV6_DUID_LENGTH = 130

class SOCKADDR(ctypes.Structure):
    _fields_ = [
        ('family', ctypes.c_ushort),
        ('data', ctypes.c_byte*14),
        ]
LPSOCKADDR = ctypes.POINTER(SOCKADDR)
class SOCKET_ADDRESS(ctypes.Structure):
    _fields_ = [
        ('address', LPSOCKADDR),
        ('length', ctypes.c_int),
        ]

class IP_ADAPTER_PREFIX(ctypes.Structure):
    pass
PIP_ADAPTER_PREFIX = ctypes.POINTER(IP_ADAPTER_PREFIX)
IP_ADAPTER_PREFIX._fields_ = [  #pylint: disable=protected-access
    ("alignment", ctypes.c_ulonglong),
    ("next", PIP_ADAPTER_PREFIX),
    ("address", SOCKET_ADDRESS),
    ("prefix_length", ctypes.c_ulong)
    ]

class IP_ADAPTER_UNICAST_ADDRESS(ctypes.Structure):
    pass
PIP_ADAPTER_UNICAST_ADDRESS = ctypes.POINTER(IP_ADAPTER_UNICAST_ADDRESS)
IP_ADAPTER_UNICAST_ADDRESS._fields_ = [ #pylint: disable=protected-access
        ("length", ctypes.c_ulong),
        ("flags", ctypes.wintypes.DWORD),
        ("next", PIP_ADAPTER_UNICAST_ADDRESS),
        ("address", SOCKET_ADDRESS),
        ("prefix_origin", ctypes.c_int),
        ("suffix_origin", ctypes.c_int),
        ("dad_state", ctypes.c_int),
        ("valid_lifetime", ctypes.c_ulong),
        ("preferred_lifetime", ctypes.c_ulong),
        ("lease_lifetime", ctypes.c_ulong),
        ("on_link_prefix_length", ctypes.c_ubyte)
        ]

class IP_ADAPTER_ADDRESSES(ctypes.Structure):
    pass
LP_IP_ADAPTER_ADDRESSES = ctypes.POINTER(IP_ADAPTER_ADDRESSES)
    
# for now, just use void * for pointers to unused structures
PIP_ADAPTER_ANYCAST_ADDRESS = ctypes.c_void_p
PIP_ADAPTER_MULTICAST_ADDRESS = ctypes.c_void_p
PIP_ADAPTER_DNS_SERVER_ADDRESS = ctypes.c_void_p
#PIP_ADAPTER_PREFIX = ctypes.c_void_p
PIP_ADAPTER_WINS_SERVER_ADDRESS_LH = ctypes.c_void_p
PIP_ADAPTER_GATEWAY_ADDRESS_LH = ctypes.c_void_p
PIP_ADAPTER_DNS_SUFFIX = ctypes.c_void_p

IF_OPER_STATUS = ctypes.c_uint # this is an enum, consider http://code.activestate.com/recipes/576415/
IF_LUID = ctypes.c_uint64

NET_IF_COMPARTMENT_ID = ctypes.c_uint32
GUID = ctypes.c_byte*16
NET_IF_NETWORK_GUID = GUID
NET_IF_CONNECTION_TYPE = ctypes.c_uint # enum
TUNNEL_TYPE = ctypes.c_uint # enum

IP_ADAPTER_ADDRESSES._fields_ = [ #pylint: disable=protected-access
    #('u', _IP_ADAPTER_ADDRESSES_U1),
        ('length', ctypes.c_ulong),
        ('interface_index', DWORD),
    ('next', LP_IP_ADAPTER_ADDRESSES),
    ('adapter_name', ctypes.c_char_p),
    ('first_unicast_address', PIP_ADAPTER_UNICAST_ADDRESS),
    ('first_anycast_address', PIP_ADAPTER_ANYCAST_ADDRESS),
    ('first_multicast_address', PIP_ADAPTER_MULTICAST_ADDRESS),
    ('first_dns_server_address', PIP_ADAPTER_DNS_SERVER_ADDRESS),
    ('dns_suffix', ctypes.c_wchar_p),
    ('description', ctypes.c_wchar_p),
    ('friendly_name', ctypes.c_wchar_p),
    ('byte', BYTE*MAX_ADAPTER_ADDRESS_LENGTH),
    ('physical_address_length', DWORD),
    ('flags', DWORD),
    ('mtu', DWORD),
    ('interface_type', DWORD),
    ('oper_status', IF_OPER_STATUS),
    ('ipv6_interface_index', DWORD),
    ('zone_indices', DWORD),
    ('first_prefix', PIP_ADAPTER_PREFIX),
    ('transmit_link_speed', ctypes.c_uint64),
    ('receive_link_speed', ctypes.c_uint64),
    ('first_wins_server_address', PIP_ADAPTER_WINS_SERVER_ADDRESS_LH),
    ('first_gateway_address', PIP_ADAPTER_GATEWAY_ADDRESS_LH),
    ('ipv4_metric', ctypes.c_ulong),
    ('ipv6_metric', ctypes.c_ulong),
    ('luid', IF_LUID),
    ('dhcpv4_server', SOCKET_ADDRESS),
    ('compartment_id', NET_IF_COMPARTMENT_ID),
    ('network_guid', NET_IF_NETWORK_GUID),
    ('connection_type', NET_IF_CONNECTION_TYPE),
    ('tunnel_type', TUNNEL_TYPE),
    ('dhcpv6_server', SOCKET_ADDRESS),
    ('dhcpv6_client_duid', ctypes.c_byte*MAX_DHCPV6_DUID_LENGTH),
    ('dhcpv6_client_duid_length', ctypes.c_ulong),
    ('dhcpv6_iaid', ctypes.c_ulong),
    ('first_dns_suffix', PIP_ADAPTER_DNS_SUFFIX),
    ]

def GetAdaptersAddresses():
    """
    Returns an iteratable list of adapters
    """ 
    size = ctypes.c_ulong()
    _GetAdaptersAddresses = _iphlpapi.GetAdaptersAddresses
    _GetAdaptersAddresses.argtypes = [
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_void_p,
        ctypes.POINTER(IP_ADAPTER_ADDRESSES),
        ctypes.POINTER(ctypes.c_ulong),
    ]
    _GetAdaptersAddresses.restype = ctypes.c_ulong
    res = _GetAdaptersAddresses(AF_INET,0,None, None,size)
    if res != 0x6f: # BUFFER OVERFLOW
        raise RuntimeError("Error getting structure length (%d)" % res)
    pointer_type = ctypes.POINTER(IP_ADAPTER_ADDRESSES)
    size.value = 50000 # reserve a lot of memory, computers with docker containers can have maaaany adapters
    tmp_buffer = ctypes.create_string_buffer(size.value)
    struct_p = ctypes.cast(tmp_buffer, pointer_type)
    res = _GetAdaptersAddresses(AF_INET,0,None, struct_p, size)
    if res != 0x0: # NO_ERROR:
        raise RuntimeError("Error retrieving table (%d)" % res)
    while struct_p:
        yield struct_p.contents
        struct_p = struct_p.contents.next


NetworkAdapterConfig = namedtuple('NetworkAdapterConfig', 'ip name friendly_name') 
# name: e.g "3Com EtherLink XL 10/100 PCI TX NIC (3C905B-TX)"
# friendly_name: e.g. "Local Area Connection 2"

def getNetworkAdapterConfigs(print_error=False):
    networkAdapters = []
    adapters = GetAdaptersAddresses()  # list all enabled adapters
    for a in adapters:
        netConnectionId = a.friendly_name
        # first check if adapter has an address. ignore it if not
        try:
            if a.first_unicast_address and a.first_unicast_address.contents:
                try:
                    fu = a.first_unicast_address.contents
                    ad = fu.address.address.contents
                    ip_int = struct.unpack('>2xI8x', ad.data)[0]
                    ip = socket.inet_ntoa(struct.pack("!I", ip_int))
                    cnf = NetworkAdapterConfig(ip,
                                               a.description,
                                               a.friendly_name)
                    networkAdapters.append(cnf)
                except:
                    if print_error:
                        print ('could not determine IP address of adapter "{}": {}'.format(netConnectionId, traceback.format_exc()))
        except:
            print('could not determine data of adapter "{}": {}'.format(netConnectionId, traceback.format_exc()))
    return networkAdapters
