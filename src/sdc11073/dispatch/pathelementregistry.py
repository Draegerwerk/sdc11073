"""Implementation of a lookup of instances by path element."""

from typing import Any

from sdc11073.exceptions import ApiUsageError, InvalidPathError
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum


class PathElementRegistry:
    """A string-to-object lookup."""

    def __init__(self):
        self._instances = {}

    def register_instance(self, path_element: str | None, instance: Any):
        """Register an instance for the given path element."""
        if path_element in self._instances:
            msg = f'Path-element "{path_element}" already registered'
            raise ApiUsageError(msg)
        self._instances[path_element] = instance

    def unregister_instance(self, path_element: str | None, instance: Any):
        """Remove the given instance if it is the one registered for the given path element.

        An unknown path element is ignored, so this can be called without knowing whether a registration was done.
        A different instance registered for the path element is kept, so that a failed registration with a
        duplicate path element cannot remove the registration that it collided with.
        """
        if self._instances.get(path_element) is instance:
            del self._instances[path_element]

    def get_instance(self, path_element: str | None) -> Any:
        """Return the instance registered for the given path element."""
        instance = self._instances.get(path_element)
        if instance is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid path {path_element}')

            raise InvalidPathError(reason=f'{path_element} not found', soap_fault=fault)
        return instance
