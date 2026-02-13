"""Functions to access mdib data structures."""

from collections.abc import Callable, Sequence

from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
from sdc11073.mdib.mdibbase import MdibBase
from sdc11073.xml_types import pm_types


def get_one_descriptor_by_type(
    mdib: MdibBase,
    coded_value: pm_types.CodedValue,
    coded_value_comparator: Callable[[pm_types.CodedValue, pm_types.CodedValue], bool],
    allow_none: bool = False,
) -> AbstractDescriptorContainer | None:
    """Get exactly one descriptor identified by its pm:Type matching the given by pm:CodedValue.

    @param mdib: The MdibBase instance to search in.
    @param coded_value: The pm:CodedValue used to search for.
    @param _coded_value_comparator: A function that compares two pm:CodedValue instances and returns True
    if they are considered equal.
    @param allow_none: If True, return None if no descriptor is found.
    If False, raise KeyError if no descriptor is found.
    @return: The descriptor that matches the pm:CodedValue with its pm:Type.
    """
    descr = get_descriptor_by_type(mdib, coded_value, coded_value_comparator, allow_empty=True)
    if allow_none and len(descr) == 0:
        return None
    if len(descr) != 1:
        msg = f'Expected exactly one descriptor for coded value {coded_value}, found {len(descr)}'
        raise KeyError(msg)
    return descr[0]


def get_descriptor_by_type(
    mdib: MdibBase,
    coded_value: pm_types.CodedValue,
    coded_value_comparator: Callable[[pm_types.CodedValue, pm_types.CodedValue], bool],
    allow_empty: bool = False,
) -> Sequence[AbstractDescriptorContainer]:
    """Get all descriptors identified by their pm:Type matching the given by pm:CodedValue.

    @param mdib: The MdibBase instance to search in.
    @param coded_value: The pm:CodedValue used to identify the descriptor by its pm:Type
    @param coded_value_comparator: A function that compares two pm:CodedValue instances and returns True
    if they are considered equal.
    @param allow_empty: If True, return an empty list if no descriptor is found.
    If False, raise KeyError if no descriptor is found.
    @return: All descriptors that have a pm:Type matching the pm:CodedValue.
    """
    with mdib.mdib_lock:
        result_list = [
            descr
            for descr in mdib.descriptions.objects
            if descr.Type is not None and coded_value_comparator(descr.Type, coded_value)
        ]
        if not allow_empty and len(result_list) == 0:
            msg = f'No descriptor found for coded value {coded_value}'
            raise KeyError(msg)

        return result_list
