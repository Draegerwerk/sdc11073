"""A minimal set of codes, used by role provider."""
import enum

from sdc11073.xml_types import pm_types


class NomenclatureCodes(pm_types.CodedValue, enum.Enum):
    """Codes that are used by included role providers."""

    MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE = '128284'
    MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE = '128285'
    MDC_OP_SET_TIME_SYNC_REF_SRC = '128505'
    MDC_ACT_SET_TIME_ZONE = '68632'
