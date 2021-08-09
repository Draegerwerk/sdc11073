import ctypes
import socket
import struct
import traceback
from collections import namedtuple
from ctypes.wintypes import DWORD, BYTE
from socket import AF_INET

_iphlpapi = ctypes.windll.iphlpapi

#################################### structures for getAdaptersAddresses #################################

MAX_ADAPTER_ADDRESS_LENGTH = 8
MAX_DHCPV6_DUID_LENGTH = 130


class SockAddr(ctypes.Structure):
    _fields_ = [
        ('family', ctypes.c_ushort),
        ('data', ctypes.c_byte * 14),
    ]


LPSOCKADDR = ctypes.POINTER(SockAddr)


class SocketAddress(ctypes.Structure):
    _fields_ = [
        ('address', LPSOCKADDR),
        ('length', ctypes.c_int),
    ]


class IpAdapterPrefix(ctypes.Structure):
    pass


PIP_ADAPTER_PREFIX = ctypes.POINTER(IpAdapterPrefix)
IpAdapterPrefix._fields_ = [  # pylint: disable=protected-access
    ("alignment", ctypes.c_ulonglong),
    ("next", PIP_ADAPTER_PREFIX),
    ("address", SocketAddress),
    ("prefix_length", ctypes.c_ulong)
]


class IpAdapterUnicastAddress(ctypes.Structure):
    pass


PIP_ADAPTER_UNICAST_ADDRESS = ctypes.POINTER(IpAdapterUnicastAddress)
IpAdapterUnicastAddress._fields_ = [  # pylint: disable=protected-access
    ("length", ctypes.c_ulong),
    ("flags", ctypes.wintypes.DWORD),
    ("next", PIP_ADAPTER_UNICAST_ADDRESS),
    ("address", SocketAddress),
    ("prefix_origin", ctypes.c_int),
    ("suffix_origin", ctypes.c_int),
    ("dad_state", ctypes.c_int),
    ("valid_lifetime", ctypes.c_ulong),
    ("preferred_lifetime", ctypes.c_ulong),
    ("lease_lifetime", ctypes.c_ulong),
    ("on_link_prefix_length", ctypes.c_ubyte)
]


class IpAdapterAddresses(ctypes.Structure):
    pass


LP_IP_ADAPTER_ADDRESSES = ctypes.POINTER(IpAdapterAddresses)

# for now, just use void * for pointers to unused structures
PipAdapterAnycastAddress = ctypes.c_void_p
PipAdapterMulticastAddress = ctypes.c_void_p
PipAdapterDnsServerAddress = ctypes.c_void_p
# PIP_ADAPTER_PREFIX = ctypes.c_void_p
PipAdapterWinsServerAddressLh = ctypes.c_void_p
PipAdapterGatewayAddressLh = ctypes.c_void_p
PipAdapterDnsSuffix = ctypes.c_void_p

IfOperStatus = ctypes.c_uint  # this is an enum, consider http://code.activestate.com/recipes/576415/
IfLuid = ctypes.c_uint64

NetIfCompartmentId = ctypes.c_uint32
GUID = ctypes.c_byte * 16
NetIfNetworkGuid = GUID
NetIfConnectionType = ctypes.c_uint  # enum
TunnelType = ctypes.c_uint  # enum

IpAdapterAddresses._fields_ = [  # pylint: disable=protected-access
    # ('u', _IP_ADAPTER_ADDRESSES_U1),
    ('length', ctypes.c_ulong),
    ('interface_index', DWORD),
    ('next', LP_IP_ADAPTER_ADDRESSES),
    ('adapter_name', ctypes.c_char_p),
    ('first_unicast_address', PIP_ADAPTER_UNICAST_ADDRESS),
    ('first_anycast_address', PipAdapterAnycastAddress),
    ('first_multicast_address', PipAdapterMulticastAddress),
    ('first_dns_server_address', PipAdapterDnsServerAddress),
    ('dns_suffix', ctypes.c_wchar_p),
    ('description', ctypes.c_wchar_p),
    ('friendly_name', ctypes.c_wchar_p),
    ('byte', BYTE * MAX_ADAPTER_ADDRESS_LENGTH),
    ('physical_address_length', DWORD),
    ('flags', DWORD),
    ('mtu', DWORD),
    ('interface_type', DWORD),
    ('oper_status', IfOperStatus),
    ('ipv6_interface_index', DWORD),
    ('zone_indices', DWORD),
    ('first_prefix', PIP_ADAPTER_PREFIX),
    ('transmit_link_speed', ctypes.c_uint64),
    ('receive_link_speed', ctypes.c_uint64),
    ('first_wins_server_address', PipAdapterWinsServerAddressLh),
    ('first_gateway_address', PipAdapterGatewayAddressLh),
    ('ipv4_metric', ctypes.c_ulong),
    ('ipv6_metric', ctypes.c_ulong),
    ('luid', IfLuid),
    ('dhcpv4_server', SocketAddress),
    ('compartment_id', NetIfCompartmentId),
    ('network_guid', NetIfNetworkGuid),
    ('connection_type', NetIfConnectionType),
    ('TunnelType', TunnelType),
    ('dhcpv6_server', SocketAddress),
    ('dhcpv6_client_duid', ctypes.c_byte * MAX_DHCPV6_DUID_LENGTH),
    ('dhcpv6_client_duid_length', ctypes.c_ulong),
    ('dhcpv6_iaid', ctypes.c_ulong),
    ('first_dns_suffix', PipAdapterDnsSuffix),
]


def get_adapters_addresses():
    """
    Returns an iteratable list of adapters
    """
    size = ctypes.c_ulong()
    _get_adapters_addresses = _iphlpapi.GetAdaptersAddresses
    _get_adapters_addresses.argtypes = [
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_void_p,
        ctypes.POINTER(IpAdapterAddresses),
        ctypes.POINTER(ctypes.c_ulong),
    ]
    _get_adapters_addresses.restype = ctypes.c_ulong
    res = _get_adapters_addresses(AF_INET, 0, None, None, size)
    if res != 0x6f:  # BUFFER OVERFLOW
        raise RuntimeError("Error getting structure length (%d)" % res)
    pointer_type = ctypes.POINTER(IpAdapterAddresses)
    size.value = 50000  # reserve a lot of memory, computers with docker containers can have maaaany adapters
    tmp_buffer = ctypes.create_string_buffer(size.value)
    struct_p = ctypes.cast(tmp_buffer, pointer_type)
    res = _get_adapters_addresses(AF_INET, 0, None, struct_p, size)
    if res != 0x0:  # NO_ERROR:
        raise RuntimeError("Error retrieving table (%d)" % res)
    while struct_p:
        yield struct_p.contents
        struct_p = struct_p.contents.next


NetworkAdapterConfig = namedtuple('NetworkAdapterConfig', 'ip name friendly_name')


# name: e.g "3Com EtherLink XL 10/100 PCI TX NIC (3C905B-TX)"
# friendly_name: e.g. "Local Area Connection 2"

def get_network_adapter_configs(print_error=False):
    network_adapters = []
    adapters = get_adapters_addresses()  # list all enabled adapters
    for adapter in adapters:
        net_connection_id = adapter.friendly_name
        # first check if adapter has an address. ignore it if not
        try:
            if adapter.first_unicast_address and adapter.first_unicast_address.contents:
                try:
                    fu_contents = adapter.first_unicast_address.contents
                    ad_contents = fu_contents.address.address.contents
                    ip_int = struct.unpack('>2xI8x', ad_contents.data)[0]
                    ip_addr = socket.inet_ntoa(struct.pack("!I", ip_int))
                    cnf = NetworkAdapterConfig(ip_addr,
                                               adapter.description,
                                               adapter.friendly_name)
                    network_adapters.append(cnf)
                except:
                    if print_error:
                        print('could not determine IP address of adapter "{}": {}'.format(net_connection_id,
                                                                                          traceback.format_exc()))
        except:
            print('could not determine data of adapter "{}": {}'.format(net_connection_id, traceback.format_exc()))
    return network_adapters
