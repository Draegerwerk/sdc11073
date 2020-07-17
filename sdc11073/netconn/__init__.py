import os

# OS dependent import
if os.name == 'posix':
    from .posixifmanager import getNetworkAdapterConfigs, GetAdaptersAddresses
elif os.name == 'nt':
    from .ntifmanagerdll import getNetworkAdapterConfigs, GetAdaptersAddresses
else:
    raise Exception('netconn does not support os "%s"' % os.name)
