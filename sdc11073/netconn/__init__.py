import ifaddr

_IP_BLACKLIST = ('0.0.0.0', None)  # None can happen if an adapter does not have any IP address


def get_ipv4_ips():
    """returns a list of ifaddr._shared.IP objects (ip, nice_name, network_prefix)"""
    ips = []
    all_adapters = ifaddr.get_adapters()
    for adapter in all_adapters:
        for adapter.ip in adapter.ips:
            if adapter.ip.is_IPv4 and adapter.ip.ip not in _IP_BLACKLIST:
                ips.append(adapter.ip)
    return ips


def get_ipv4_addresses():
    """returns ip addresses"""
    return [ip.ip for ip in get_ipv4_ips()]


def get_ip_for_adapter(adapter_name):
    """returns a single ip address or None"""
    ip_objects = get_ipv4_ips()
    for ip_object in ip_objects:
        if ip_object.nice_name == adapter_name:
            return ip_object.ip
    return None
