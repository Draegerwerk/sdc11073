from dataclasses import dataclass
import netifaces

@dataclass(frozen=True)
class Adapter():
    friendly_name: str
    ip: str   #pylint: disable=invalid-name


def get_network_adapter_configs():
    interfaces = netifaces.interfaces()
    adapters = []
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if netifaces.AF_INET in addresses:
            adptr = Adapter(interface, addresses[netifaces.AF_INET][0]['addr'])
            adapters.append(adptr)
    return adapters


def get_adapters_addresses():
    interfaces = netifaces.interfaces()
    adapters = []
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if netifaces.AF_INET in addresses:
            adptr = Adapter(interface, addresses[netifaces.AF_INET][0]['addr'])
            adapters.append(adptr)
    return adapters
