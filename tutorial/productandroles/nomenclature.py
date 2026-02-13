"""A minimal set of codes, used by role provider."""

from sdc11073.xml_types import pm_types


class NomenclatureCodes:
    """Codes that are used by included role providers."""

    MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE = pm_types.CodedValue('128284')
    MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE = pm_types.CodedValue('128285')
    MDC_OP_SET_TIME_SYNC_REF_SRC = pm_types.CodedValue('128505')
    MDC_ACT_SET_TIME_ZONE = pm_types.CodedValue('68632')
