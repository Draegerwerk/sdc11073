"""
A minimal set of codes, used by role provider.
"""

class NomenclatureCodes(object):
    # only a small subset of all codes, these are used for tests
    MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE = 128284 # An operation to initiate global all audio pause
    MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE = 128285 # An operation to cancel global all audio pause
    MDC_OP_SET_TIME_SYNC_REF_SRC = 128505 # An operation to Set the active reference source of a clock for time synchronization.
    OP_SET_NTP = 194041  # deprecated REF_ID use MDC_OP_SET_TIME_SYNC_REF_SRC, needed for backwards compatibility
    OP_SET_TZ = 194040  # deprecated REF_ID use MDC_ACT_SET_TIME_ZONE, needed for backwards compatibility
    MDC_ACT_SET_TIME_ZONE = 68632 # replaces private Code MDC_OP_SET_TIME_ZONE (194040)
