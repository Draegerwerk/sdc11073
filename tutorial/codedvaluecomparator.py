"""Example utility functions to compare coded values."""

from sdc11073.xml_types import pm_types

DEFAULT_CODING_SYSTEM = 'urn:oid:1.2.840.10004.1.1.1.0.0.1'  # ISO/IEC 11073-10101


def _list_of_codes_equal(
    list1: list[pm_types.CodedValue],
    list2: list[pm_types.CodedValue],
    default_coding_system: str = DEFAULT_CODING_SYSTEM,
) -> bool:
    """Compare two lists of coded values, do not consider translations."""
    if len(list1) != len(list2):
        return False

    list2_copy = list2.copy()

    for cv1 in list1:
        found = False
        for cv2 in list2_copy:
            if _check_equal_codes(cv1, cv2, default_coding_system):
                found = True
                list2_copy.remove(cv2)
                break
        if not found:
            return False

    return not list2_copy


def _coded_value_comparator(
    cv1: pm_types.CodedValue,
    cv2: pm_types.CodedValue,
    default_coding_system: str = DEFAULT_CODING_SYSTEM,
) -> bool:
    """Compare coded values, considering translations."""
    for code1 in [cv1, *cv1.Translation]:
        for code2 in [cv2, *cv2.Translation]:
            if _check_equal_codes(code1, code2, default_coding_system):
                return True
    return False


def _check_equal_codes(
    code1: pm_types.CodedValue | pm_types.Translation,
    code2: pm_types.CodedValue | pm_types.Translation,
    default_coding_system: int | str = DEFAULT_CODING_SYSTEM,
) -> bool:
    return (
        code1.Code == code2.Code
        and (code1.CodingSystem if code1.CodingSystem is not None else default_coding_system)
        == (code2.CodingSystem if code2.CodingSystem is not None else default_coding_system)
        and code1.CodingSystemVersion == code2.CodingSystemVersion
    )
