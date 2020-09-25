import os

# OS dependent import
if os.name == 'nt':
    from .ntifmanagerdll import getNetworkAdapterConfigs, GetAdaptersAddresses
else:
    from .posixifmanager import getNetworkAdapterConfigs, GetAdaptersAddresses
