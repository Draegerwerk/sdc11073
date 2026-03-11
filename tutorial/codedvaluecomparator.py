"""Utility functions for comparing CodedValue instances. It is only intended for unit testing the sdc11073 library.

For strict compliance with BICEPS IEEE 11073-10207-2017 and BICEPS IEEE 11073-10207-2017/Cor 1-2025,
a dedicated comparison function must be used when evaluating CodedValue instances.
Each user is responsible for defining their own comparison logic to satisfy their particular requirements.
"""

from collections.abc import Sequence

from sdc11073.xml_types import pm_types

DEFAULT_CODING_SYSTEM = 'urn:oid:1.2.840.10004.1.1.1.0.0.1'  # ISO/IEC 11073-10101


def _list_of_codes_equal(
    list1: Sequence[pm_types.CodedValue],
    list2: Sequence[pm_types.CodedValue],
    default_coding_system: str = DEFAULT_CODING_SYSTEM,
) -> bool:
    """Compare two lists of coded values, do not consider translations.

    It is not needed that the codes are in the same order,
    but each code in list1 must be matched with exactly one code in list2.
    """
    if len(list1) != len(list2):
        return False

    list2_copy = list(list2)

    for cv1 in list1:
        found = False
        for cv2 in list2_copy:
            if _check_equal_codes(cv1, cv2, default_coding_system):
                found = True
                list2_copy.remove(cv2)
                break
        if not found:
            # if a code in list1 cannot be matched with any code in list2, the lists are not equal
            return False

    # when list2_copy is empty, all codes in list1 have been matched with a code in list2
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
    default_coding_system: str = DEFAULT_CODING_SYSTEM,
) -> bool:
    # Hint: implied value for CodingSystem is only used if CodingSystem is None,
    # an empty string is interpreted as an explicitly defined value,
    return (
        code1.Code == code2.Code
        and (code1.CodingSystem if code1.CodingSystem is not None else default_coding_system)
        == (code2.CodingSystem if code2.CodingSystem is not None else default_coding_system)
        and code1.CodingSystemVersion == code2.CodingSystemVersion
    )
