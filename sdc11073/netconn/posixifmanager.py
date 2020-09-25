import netifaces


class Adapter():
    friendly_name = ""
    ip = ""


def getNetworkAdapterConfigs():
    interfaces = netifaces.interfaces()
    adapters = []
    for ad in interfaces:
        addresses = netifaces.ifaddresses(ad)
        if netifaces.AF_INET in addresses:
            adptr = Adapter()
            adptr.ip = addresses[netifaces.AF_INET][0]['addr']
            adptr.friendly_name = ad
            adapters.append(adptr)
    return adapters


def GetAdaptersAddresses():
    interfaces = netifaces.interfaces()
    adapters = []
    for ad in interfaces:
        addresses = netifaces.ifaddresses(ad)
        if netifaces.AF_INET in addresses:
            adptr = Adapter()
            adptr.ip = addresses[netifaces.AF_INET][0]['addr']
            adptr.friendly_name = ad
            adapters.append(adptr)
    return adapters
