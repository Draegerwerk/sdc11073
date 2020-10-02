from . import pysoap
from . import safety
from . import namespaces
from . import wsdiscovery
from . import mdib
from . import netconn
from . import pmtypes
from . import location
from . import sdcclient
from . import sdcdevice
from . import xmlparsing
from . import commlog
from . import compression

try:
    from . import version
    __version__ = version.version
except ImportError:
    __version__ = '0.0.0'
