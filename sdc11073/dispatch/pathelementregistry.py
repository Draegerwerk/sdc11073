from typing import Union, Any
from ..pysoap.soapenvelope import SoapFault, FaultCodeEnum
from ..exceptions import ApiUsageError
from ..exceptions import InvalidPathError

class PathElementRegistry:
    """ A string - object lookup"""

    def __init__(self):
        self._instances = {}

    def register_instance(self, path_element: Union[str, None], instance: Any):
        if path_element in self._instances:
            raise ApiUsageError(f'Path-element "{path_element}" already registered')
        self._instances[path_element] = instance

    def get_instance(self, path_element: Union[str, None]) -> Any:
        instance = self._instances.get(path_element)
        if instance is None:
            soap_fault = SoapFault(code=FaultCodeEnum.SENDER, reason=f'invalid path {path_element}')
            raise InvalidPathError(reason=f'{path_element} not found', soap_fault=soap_fault)
        return instance
