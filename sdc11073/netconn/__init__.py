import platform

# OS dependent import
if platform.system() == 'Windows':
    from .ntifmanagerdll import getNetworkAdapterConfigs, GetAdaptersAddresses
else:
    from .posixifmanager import getNetworkAdapterConfigs, GetAdaptersAddresses
