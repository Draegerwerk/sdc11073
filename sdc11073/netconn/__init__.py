import platform

# OS dependent import
if platform.system() == 'Windows':
    from .ntifmanagerdll import get_network_adapter_configs, get_adapters_addresses
else:
    from .posixifmanager import get_network_adapter_configs, get_adapters_addresses
