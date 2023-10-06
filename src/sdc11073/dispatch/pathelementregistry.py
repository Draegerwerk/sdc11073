from typing import Union, Any
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum
from sdc11073.exceptions import ApiUsageError
from sdc11073.exceptions import InvalidPathError

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
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid path {path_element}')

            raise InvalidPathError(reason=f'{path_element} not found', soap_fault=fault)
        return instance
