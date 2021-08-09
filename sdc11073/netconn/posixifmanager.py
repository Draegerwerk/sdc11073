import netifaces


class Adapter():
    friendly_name = ""
    ip = ""


def get_network_adapter_configs():
    interfaces = netifaces.interfaces()
    adapters = []
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if netifaces.AF_INET in addresses:
            adptr = Adapter()
            adptr.ip = addresses[netifaces.AF_INET][0]['addr']
            adptr.friendly_name = interface
            adapters.append(adptr)
    return adapters


def get_adapters_addresses():
    interfaces = netifaces.interfaces()
    adapters = []
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if netifaces.AF_INET in addresses:
            adptr = Adapter()
            adptr.ip = addresses[netifaces.AF_INET][0]['addr']
            adptr.friendly_name = interface
            adapters.append(adptr)
    return adapters
