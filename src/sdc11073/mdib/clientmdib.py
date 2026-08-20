import threading
import traceback
import time

from threading import Lock
from collections import deque
from collections import namedtuple
from statistics import mean, stdev
import copy
from lxml import etree as etree_
from .. import observableproperties as properties
from . import mdibbase
from . import msgreader
from .. import namespaces
from .. import pmtypes
from .. import loghelper
from .. import xmlparsing

_global_nsmap = namespaces.nsmap

PROFILING = False
if PROFILING:
    import cProfile
    import pstats
    from io import StringIO


LOG_WF_AGE_INTERVAL = 30 # how often a log message is written with mean and stdef of waveforms age
AGE_CALC_SAMPLES_COUNT = 100 # amount of data for wf mean age and stdev calculation

A_NO_LOG = 0
A_OUT_OF_RANGE = 1
A_STILL_OUT_OF_RANGE = 2
A_BACK_IN_RANGE = 3


class DeterminationTimeWarner:
    """A Helper to reduce log warnings regarding determination time."""
    ST_IN_RANGE = 0
    ST_OUT_OF_RANGE = 1
    result_lookup = {
        # (last, current) :  (action, shall_repeat)
        (ST_IN_RANGE, ST_IN_RANGE): (A_NO_LOG, False),
        (ST_IN_RANGE, ST_OUT_OF_RANGE): (A_OUT_OF_RANGE, False),
        (ST_OUT_OF_RANGE, ST_OUT_OF_RANGE): (A_STILL_OUT_OF_RANGE, True),
        (ST_OUT_OF_RANGE, ST_IN_RANGE): (A_BACK_IN_RANGE, False)
    }
    def __init__(self, repeat_period=30):
        self.repeat_period = repeat_period
        self._last_log_time = 0
        self.last_state = self.ST_IN_RANGE

    def getOutOfDeterminationTimeLogState(self, minAge, maxAge, warn_limit):
        """
        @return: one of above constants
        """
        now = time.time()
        if minAge < -warn_limit or maxAge > warn_limit:
            current_state = self.ST_OUT_OF_RANGE
        else:
            current_state = self.ST_IN_RANGE
        action, shall_repeat = self.result_lookup[(self.last_state, current_state)]
        if self.last_state  != current_state:
            # a state transition
            self.last_state = current_state
            self._last_log_time = now
            return action
        else:
            # no state transition, but might need repeated logging
            if shall_repeat and now - self._last_log_time >= self.repeat_period:
                self._last_log_time = now
                return action
            else:
                return A_NO_LOG

_AgeData = namedtuple('_AgeData', 'mean_age stdev min_age max_age')

class ClientRtBuffer(object):
    """Collects data of one real time stream."""
    def __init__(self, sample_period, max_samples):
        """
        :param sample_period: float value, in seconds. 
                              When an incoming real time sample array is split into single RtSampleContainers, this is used to calculate the individual time stamps.
                              Value can be zero if correct value is not known. In this case all Containers will have the observation time of the sample array.
        :param max_samples: integer, max. length of self.rtdata
        """
        self.rt_data = deque(maxlen=max_samples)
        self.sample_period = sample_period
        self._max_samples = max_samples
        self._logger = loghelper.getLoggerAdapter('sdc.client.mdib.rt')
        self._lock = Lock()
        self.last_sc = None  # last statecontainer that was handled
        self._age_of_data_list = deque(maxlen=AGE_CALC_SAMPLES_COUNT) # used to calculate average age of samples when received
        self._reported_min_age = None
        self._reported_max_age = None


    def mkRtSampleContainers(self, realtimeSampleArrayContainer):
        """

        :param realtimeSampleArrayContainer: a RealTimeSampleArrayMetricStateContainer instance
        :return: a list of mdibbase.RtSampleContainer
        """
        self.last_sc = realtimeSampleArrayContainer
        metricValue = realtimeSampleArrayContainer.metricValue
        if metricValue is None:
            # this can happen if metric state is not activated.
            self._logger.debug('real time sample array "{} "has no metric value, ignoring it', realtimeSampleArrayContainer.descriptorHandle)
            return []
        observationTime = metricValue.DeterminationTime
        annotations = metricValue.Annotations
        applyAnnotations = metricValue.ApplyAnnotations
        rtSampleContainers = []
        if metricValue.Samples is not None:
            for i, sample in enumerate(metricValue.Samples):
                appliedAnnotations = []
                if applyAnnotations is not None:
                    for aa in applyAnnotations:
                        if aa.SampleIndex == i:
                            # there is an annotation for this sample:
                            aIndex = aa.AnnotationIndex
                            annot = annotations[aIndex] # index is zero-based
                            appliedAnnotations.append(annot)
                t = observationTime + i * self.sample_period
                rtSampleContainers.append(mdibbase.RtSampleContainer(sample, t, metricValue.Validity, appliedAnnotations))
        return rtSampleContainers

    def addRtSampleContainers(self, sc):
        if not sc:
            return
        with self._lock:
            self.rt_data.extend(sc)
            self._age_of_data_list.append(time.time() - sc[-1].observationTime) # use time of youngest sample, this is the best value for indication of delays
        try:
            self._reported_min_age = min(self._age_of_data_list[-1], self._reported_min_age)
        except TypeError:
            self._reported_min_age = self._age_of_data_list[-1]
        try:
            self._reported_max_age = max(self._age_of_data_list[-1], self._reported_min_age)
        except TypeError:
            self._reported_max_age = self._age_of_data_list[-1]

    def readData(self):
        """ This read method consumes all data in buffer.
        @return: a list of RtSampleContainer objects"""    
        with self._lock:
            ret = copy.copy(self.rt_data)
            self.rt_data.clear()
        return ret


    def get_age_stdev(self):
        with self._lock:
            min_value, self._reported_min_age = self._reported_min_age, None
            max_value, self._reported_max_age = self._reported_max_age, None
            mean_data = 0 if len(self._age_of_data_list) == 0 else mean(self._age_of_data_list)
            std_deviation = 0 if len(self._age_of_data_list) < 2 else stdev(self._age_of_data_list)
            return _AgeData(mean_data, std_deviation, min_value or 0, max_value or 0)


MDIB_VERSION_TOO_OLD = '{}: MDIB not yet synchronized. This MdibVersion will not be processed, current {}, received {}'
MDIB_VERSION_UNEXPECTED = '{}: unexpect MdibVersion, expected {}, received {}'

_BufferedNotification = namedtuple('_BufferedNotification', 'report handler')


class ClientMdibContainer(mdibbase.MdibContainer):
    """ This mdib is meant to be read-only.
    Only update source is a BICEPSClient."""

    DETERMINATIONTIME_WARN_LIMIT = 1.0 # in seconds
    INITIAL_NOTIFICATION_BUFFERING = True # if False, the response for the first incoming notification is answered after the getmdib is done.
                                          # if True, first notifications are buffered and the responses are sent immediately.
    def __init__(self, sdcClient, maxRealtimeSamples=100):
        super(ClientMdibContainer, self).__init__(sdcClient.sdc_definitions)
        self._synchronizedReports = threading.Event()
        self._logger = loghelper.getLoggerAdapter('sdc.client.mdib', sdcClient.log_prefix)
        self._sdcClient = sdcClient
        self._isInitialized = False
        self.rtBuffers = {}  # key  is a handle, value is a ClientRtBuffer
        self._maxRealtimeSamples = maxRealtimeSamples
        self._last_wf_age_log = time.time()
        if PROFILING:
            self.pr = cProfile.Profile()
        
        self._contextMdibVersion = None
        self._msgReader = msgreader.MessageReader(self)
        # a buffer for notifications that are received before initial getmdib is done
        self._bufferedNotifications = list()
        self._bufferedNotificationsLock = Lock()
        self.waveform_time_warner = DeterminationTimeWarner()
        self.metric_time_warner = DeterminationTimeWarner()

    def initMdib(self):
        if  self._isInitialized:
            raise RuntimeError('ClientMdibContainer is already initialized')
        # first start receiving notifications, then call getMdib.
        # Otherwise we might miss notifications.
        self._bindToObservables()
        
        getService = self._sdcClient.client('Get')
        self._logger.info('initializing mdib...')
        mdibNode = getService.getMdibNode()
        self.nsmapper.useDocPrefixes(mdibNode.nsmap)
        self._logger.info('creating description containers...')
        descriptorContainers = self._msgReader.readMdDescription(mdibNode)
        with self.descriptions._lock: #pylint: disable=protected-access
            self.descriptions.clear()
        self.addDescriptionContainers(descriptorContainers)
        self._logger.info('creating state containers...')
        self.clearStates()
        stateContainers = self._msgReader.readMdState(mdibNode)
        self.addStateContainers(stateContainers)

        mdibVersion = mdibNode.get('MdibVersion')
        sequenceId = mdibNode.get('SequenceId')
        instanceId = mdibNode.get('InstanceId')
        if mdibVersion is not None:
            self.mdibVersion = int(mdibVersion)
            self._logger.info('setting initial mdib version to {}', mdibVersion)
        else:
            self._logger.warn('found no mdib version in GetMdib response, assuming "0"')
            self.mdibVersion = 0
        self.sequenceId = sequenceId
        self._logger.info('setting initial sequence Id to {}', sequenceId)

        if instanceId is not None:
            self.instanceId = int(instanceId)
            self._logger.info('setting initial instance id  to {}', instanceId)

        # retrieve context states only if there were none in mdibNode
        if len(self.contextStates.objects) == 0:
            self._retrieve_context_states()
        else:
            self._logger.info('found context states in GetMdib Result, will not call getContextStates')

        # process buffered notifications
        with self._bufferedNotificationsLock:
            for bufferedReport in self._bufferedNotifications:
                bufferedReport.handler(bufferedReport.report, is_buffered_report=True)
            del self._bufferedNotifications[:]
            self._isInitialized = True

        self._sdcClient._register_mdib(self) #pylint: disable=protected-access
        self._logger.info('initializing mdib done')


    def _bufferNotification(self, report, callable):
        """
        write notification to an temporary buffer, as long as mdib is not initialized
        :param report: the report
        :param callable: the mothod that shall be called later for delayed handling of report
        :return: True if buffered, False if report shall be processed immediately
        """
        if self._isInitialized:
            # no reason to buffer
            return False

        if not self.INITIAL_NOTIFICATION_BUFFERING:
            self._waitUntilInitialized(callable.__name__)
            return False

        # get lock and check if we need to write to buffer
        with self._bufferedNotificationsLock:
            if not self._isInitialized:
                self._bufferedNotifications.append(_BufferedNotification(report, callable))
                return True
            return False

    def syncContextStates(self):
        """This method requests all context states from device and deletes all local context states that are not
        available in response from Device."""
        try:
            self._logger.info('syncContextStates called')
            contextService = self._sdcClient.client('Context')
            responseNode = contextService.getContextStatesNode()
            self._logger.info('creating context state containers...')
            contextStateContainers = self._msgReader.readContextState(responseNode)
            devices_contextStateHandles = [s.Handle for s in contextStateContainers]
            with self.contextStates._lock:  # pylint: disable=protected-access
                for obj in self.contextStates.objects:
                    if obj.Handle not in devices_contextStateHandles:
                        self.contextStates.removeObjectNoLock((obj))
        except:
            self._logger.error(traceback.format_exc())


    def _retrieve_context_states(self, handles = None):
        """Only called when context states are not included in GetMdib response."""
        self._logger.debug('new Query, handles={}', handles)
        contextService = self._sdcClient.client('Context')
        self._logger.info('requesting context states...')
        responseNode = contextService.getContextStatesNode(handles)
        self._logger.info('creating context state containers...')
        context_state_containers = self._msgReader.readContextState(responseNode)

        self._contextMdibVersion = int(responseNode.get('MdibVersion', '0'))
        self._logger.debug('_getContextStates: setting _contextMdibVersion to {}', self._contextMdibVersion)

        self._logger.debug('got {} context states', len(context_state_containers))
        with self.contextStates._lock: #pylint: disable=protected-access
            for stateContainer in context_state_containers:
                old_state_containers = self.contextStates.handle.get(stateContainer.Handle, [])
                if not len(old_state_containers):
                    self.contextStates.addObjectNoLock(stateContainer)
                    self._logger.debug('new ContextState {}', stateContainer)
                else:
                    txt = ', '.join([str(x) for x in old_state_containers])
                    msg = (
                        f'Unexpected behavior. ContextState(s) are already included in the MDIB. '
                        f'Found {len(old_state_containers)} objects: {txt}'
                    )
                    raise RuntimeError(msg)

    def _bindToObservables(self):
        # observe properties of sdcClient
        if PROFILING:
            properties.bind(self._sdcClient, waveFormReport=self._onWaveformReportProfiled)
        else:
            properties.bind(self._sdcClient, waveFormReport=self._onWaveformReport)
        properties.bind(self._sdcClient, episodicMetricReport=self._onEpisodicMetricReport)
        properties.bind(self._sdcClient, episodicAlertReport=self._onEpisodicAlertReport)
        properties.bind(self._sdcClient, episodicContextReport=self._onEpisodicContextReport)
        properties.bind(self._sdcClient, episodicComponentReport=self._onEpisodicComponentReport)
        properties.bind(self._sdcClient, descriptionModificationReport=self._onDescriptionModificationReport)
        properties.bind(self._sdcClient, episodicOperationalStateReport=self._onOperationalStateReport)


    def _canAcceptMdibVersion(self, log_prefix, mdib_version):
        if mdib_version <= 0:
            # It is not assumed that MDIB version within a notification is zero.
            # This is only possible in GetMdib response.
            msg = f'{log_prefix}: MdibVersion is {mdib_version}, must be greater than 0!'
            raise ValueError(msg)

        # SDPi R1007 requires a strictly increasing msg:AbstractReport/@MdibVersion.
        # This prohibits decrementing version numbers within an MDIB sequence.
        if mdib_version < self.mdibVersion:
            if self._synchronizedReports.is_set():
                msg = (f'{log_prefix}: received MdibVersion {mdib_version} is older than the current '
                       f'MdibVersion {self.mdibVersion}!')
                raise ValueError(msg)
            else:
                self._logger.debug(MDIB_VERSION_TOO_OLD, log_prefix, self.mdibVersion, mdib_version)
                return False

        # SDPi R1007 requires a strictly increasing msg:AbstractReport/@MdibVersion.
        # This prohibits two reports with the same MDIB version.
        elif mdib_version == self.mdibVersion:
            if self._synchronizedReports.is_set():
                msg = (f'{log_prefix}: received MdibVersion {mdib_version} equals the current '
                       f'MdibVersion!')
                raise ValueError(msg)
            else:
                self._synchronizedReports.set()
                return False

        elif (mdib_version - self.mdibVersion) > 1:
            # An error is logged in this case because this MDIB implementation cannot determine whether essential
            # information was missed or not received.
            self._logger.error(MDIB_VERSION_UNEXPECTED, log_prefix, self.mdibVersion + 1, mdib_version)
            if self._sdcClient.all_subscribed:
                msg = (f'{log_prefix}: received MdibVersion {mdib_version} skips one or more versions '
                       f'(expected {self.mdibVersion + 1})!')
                raise ValueError(msg)

        if mdib_version > self.mdibVersion:
            self._synchronizedReports.set()
            return True

        raise RuntimeError('THIS SHOULD NEVER HAPPEN!')


    def _update_mdib_version_group(self, reportNode):
        self.mdibVersion = int(reportNode.get('MdibVersion', '0'))

        sequenceId = reportNode.get('SequenceId')
        if sequenceId != self.sequenceId:
            self.sequenceId = sequenceId

        instance_id = reportNode.get('InstanceId')
        if instance_id != self.instanceId:
            self.instanceId = int(instance_id)


    def _extract_mdib_version(self, report_node):
        try:
            return int(report_node.get('MdibVersion'))
        except (TypeError, ValueError):
            raise RuntimeError('MdibVersion must be an integer.')


    def _waitUntilInitialized(self, log_prefix):
        showsuccesslog = False
        started = time.monotonic()
        while not self._isInitialized:
            delay = time.monotonic() - started
            if 3 >= delay > 1:
                showsuccesslog = True
                self._logger.warn('{}: _waitUntilInitialized takes long...', log_prefix)
            elif delay > 10:
                raise RuntimeError('_waitUntilInitialized failed')
            time.sleep(1)
        delay = time.monotonic() - started
        if showsuccesslog:
            self._logger.info('{}: _waitUntilInitialized took {} seconds', log_prefix, delay)


    def _onEpisodicMetricReport(self, reportNode, is_buffered_report=False):
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        if not is_buffered_report and self._bufferNotification(reportNode, self._onEpisodicMetricReport):
            return

        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onEpisodicMetricReport', mdib_version):
            return

        now = time.time()
        metricsByHandle = {}
        maxAge = 0
        minAge = 0
        statecontainers = self._msgReader.readEpisodicMetricReport(reportNode)
        try:
            with self.mdibLock:
                for sc in statecontainers:
                    if sc.descriptorContainer is not None and sc.descriptorContainer.DescriptorVersion != sc.DescriptorVersion:
                        self._logger.warn(
                            '_onEpisodicMetricReport: metric "{}": descriptor version expect "{}", found "{}"',
                            sc.descriptorHandle, sc.DescriptorVersion, sc.descriptorContainer.DescriptorVersion)
                        sc.descriptorContainer = None
                    try:
                        oldStateContainer = self.states.descriptorHandle.getOne(sc.descriptorHandle, allowNone=True)
                    except RuntimeError  as ex:
                        self._logger.error('_onEpisodicMetricReport, getOne on states: {}', ex)
                        continue

                    if oldStateContainer is not None:
                        if self._hasNewStateUsableStateVersion(oldStateContainer, sc, 'EpisodicMetricReport', is_buffered_report):
                            oldStateContainer.updateFromOtherContainer(sc)
                            self.states.updateObject(oldStateContainer)
                            metricsByHandle[oldStateContainer.descriptorHandle] = oldStateContainer
                    else:
                        self.states.addObject(sc)
                        metricsByHandle[sc.descriptorHandle] = sc

                    if sc.metricValue is not None:
                        # BICEPS: While Validity is "Ong" or "NA", the enclosing METRIC value SHALL not possess a determined value
                        # Also ignore determination time if measurement is invalid.
                        if sc.metricValue.Validity not in [pmtypes.MeasurementValidity.INVALID,
                                                           pmtypes.MeasurementValidity.NA,
                                                           pmtypes.MeasurementValidity.MEASUREMENT_ONGOING]:
                            observationTime = sc.metricValue.DeterminationTime
                            if observationTime is None:
                                self._logger.warn(
                                    '_onEpisodicMetricReport: metric {} version {} has no DeterminationTime',
                                    sc.descriptorHandle, sc.StateVersion)
                            else:
                                age = now - observationTime
                                minAge = min(minAge, age)
                                maxAge = max(maxAge, age)

                self._update_mdib_version_group(reportNode)

            shall_log = self.metric_time_warner.getOutOfDeterminationTimeLogState(minAge, maxAge, self.DETERMINATIONTIME_WARN_LIMIT)
            if shall_log == A_OUT_OF_RANGE:
                self._logger.warn(
                    '_onEpisodicMetricReport mdibVersion {}: age of metrics outside limit of {} sec.: max, min = {:03f}, {:03f}',
                    mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, maxAge, minAge)
            elif shall_log == A_STILL_OUT_OF_RANGE:
                self._logger.warn(
                    '_onEpisodicMetricReport mdibVersion {}: age of metrics still outside limit of {} sec.: max, min = {:03f}, {:03f}',
                    mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, maxAge, minAge)
            elif shall_log == A_BACK_IN_RANGE:
                self._logger.info(
                    '_onEpisodicMetricReport mdibVersion {}: age of metrics back in limit of {} sec.: max, min = {:03f}, {:03f}',
                    mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, maxAge, minAge)
        finally:
            self._updateStateObservables(metricsByHandle.values())

    def _onEpisodicAlertReport(self, reportNode, is_buffered_report=False):
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        if not is_buffered_report and self._bufferNotification(reportNode, self._onEpisodicAlertReport):
            return
        
        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onEpisodicAlertReport', mdib_version):
            return

        alertByHandle = {}
        allAlertContainers = self._msgReader.readEpisodicAlertReport(reportNode)
        self._logger.debug('_onEpisodicAlertReport: received {} alerts', len(allAlertContainers))
        try:
            with self.mdibLock:
                for sc in allAlertContainers:
                    if sc.descriptorContainer is not None and sc.descriptorContainer.DescriptorVersion != sc.DescriptorVersion:
                        self._logger.warn(
                            '_onEpisodicAlertReport: alert "{}": descriptor version expect "{}", found "{}"',
                            sc.descriptorHandle, sc.DescriptorVersion, sc.descriptorContainer.DescriptorVersion)
                        sc.descriptorContainer = None
                    try:
                        oldStateContainer = self.states.descriptorHandle.getOne(sc.descriptorHandle, allowNone=True)
                    except RuntimeError as ex:
                        self._logger.error('_onEpisodicAlertReport, getOne on states: {}', ex)
                        continue

                    if oldStateContainer is not None:
                        if self._hasNewStateUsableStateVersion(oldStateContainer, sc, 'EpisodicAlertReport', is_buffered_report):
                            oldStateContainer.updateFromOtherContainer(sc)
                            self.states.updateObject(oldStateContainer)
                            alertByHandle[oldStateContainer.descriptorHandle] = oldStateContainer
                    else:
                        self.states.addObject(sc)
                        alertByHandle[sc.descriptorHandle] = sc
                self._update_mdib_version_group(reportNode)
        finally:
            self._updateStateObservables(alertByHandle.values())

    def _onOperationalStateReport(self, reportNode, is_buffered_report=False):
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        if not is_buffered_report and self._bufferNotification(reportNode, self._onOperationalStateReport):
            return

        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onOperationalStateReport', mdib_version):
            return
        
        operationByHandle = {}
        self._logger.info('_onOperationalStateReport: report={}', lambda:etree_.tostring(reportNode))
        allOperationStateContainers = self._msgReader.readOperationalStateReport(reportNode)
        try:
            with self.mdibLock:
                for sc in allOperationStateContainers:
                    if sc.descriptorContainer is not None and sc.descriptorContainer.DescriptorVersion != sc.DescriptorVersion:
                        self._logger.warn('_onOperationalStateReport: OperationState "{}": descriptor version expect "{}", found "{}"',
                                          sc.descriptorHandle, sc.DescriptorVersion, sc.descriptorContainer.DescriptorVersion)
                        sc.descriptorContainer = None
                    try:
                        oldStateContainer = self.states.descriptorHandle.getOne(sc.descriptorHandle, allowNone=True)
                    except RuntimeError as ex:
                        self._logger.error('_onOperationalStateReport, getOne on states: {}', ex)
                        continue

                    if oldStateContainer is not None:
                        if self._hasNewStateUsableStateVersion(oldStateContainer, sc, 'OperationalStateReport', is_buffered_report):
                            oldStateContainer.updateFromOtherContainer(sc)
                            self.states.updateObject(oldStateContainer)
                            operationByHandle[oldStateContainer.descriptorHandle] = oldStateContainer
                    else:
                        self.states.addObject(sc)
                        operationByHandle[sc.descriptorHandle] = sc
                self._update_mdib_version_group(reportNode)
        finally:
            self._updateStateObservables(operationByHandle.values())

    def _onWaveformReportProfiled(self, reportNode):
        self.pr.enable()
        self._onWaveformReport(reportNode)
        self.pr.disable()
        s = StringIO()
        ps = pstats.Stats(self.pr, stream=s).sort_stats('cumulative')
        ps.print_stats(30)
        print (s.getvalue())
        print ('total number of states: {}'.format(len(self.states._objects))) #pylint:disable=protected-access
        print ('total number of objIds: {}'.format(len(self.states._objectIDs))) #pylint:disable=protected-access
        for name, l in self.states._objectIDs.items(): #pylint:disable=protected-access
            if len(l) > 50:
                print ('object {} has {} idx references, {}'.format(name, len(l), l))


    def _onWaveformReport(self, reportNode, is_buffered_report=False):
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        #pylint:disable=too-many-locals
        # reportNode contains a list of msg:State nodes
        if not is_buffered_report and self._bufferNotification(reportNode, self._onWaveformReport):
            return

        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onWaveformReport', mdib_version):
            return
        
        waveformByHandle = {}
        waveformAge = {} # collect age of all waveforms in this report, and make one report if age is above warn limit (instead of multiple)
        allRtSampleArrayContainers = self._msgReader.readWaveformReport(reportNode)
        self._logger.debug('_onWaveformReport: {} waveforms received', len(allRtSampleArrayContainers))
        try:
            with self.mdibLock:
                for new_sac in allRtSampleArrayContainers:
                    d_handle = new_sac.descriptorHandle
                    descriptorContainer = new_sac.descriptorContainer
                    if descriptorContainer is None:
                        self._logger.warn('_onWaveformReport: No Descriptor found for handle "{}"', d_handle)

                    oldStateContainer = self.states.descriptorHandle.getOne(d_handle, allowNone=True)
                    if oldStateContainer is not None:
                        if self._hasNewStateUsableStateVersion(oldStateContainer, new_sac, 'WaveformReport', is_buffered_report):
                            oldStateContainer.updateFromOtherContainer(new_sac)
                            self.states.updateObject(oldStateContainer)
                            waveformByHandle[oldStateContainer.descriptorHandle] = oldStateContainer
                    else:
                        self.states.addObject(new_sac)
                        waveformByHandle[new_sac.descriptorHandle] = new_sac

                    # add to Waveform Buffer
                    rtBuffer = self.rtBuffers.get(d_handle)
                    if rtBuffer is None:
                        if descriptorContainer is not None:
                            # read sample period
                            try:
                                sample_period = descriptorContainer.SamplePeriod or 0
                            except AttributeError:
                                sample_period = 0  # default
                        rtBuffer = ClientRtBuffer(sample_period=sample_period, max_samples=self._maxRealtimeSamples)
                        self.rtBuffers[d_handle] = rtBuffer

                    rtSampleContainers = rtBuffer.mkRtSampleContainers(new_sac)
                    rtBuffer.addRtSampleContainers(rtSampleContainers)

                    # check age
                    if len(rtSampleContainers) > 0:
                        waveformAge[d_handle] = rtSampleContainers[-1].age

                    # check descriptor version
                    if descriptorContainer.DescriptorVersion != new_sac.DescriptorVersion:
                        self._logger.error('_onWaveformReport: descriptor {}: expect version "{}", found "{}"',
                                          d_handle, new_sac.DescriptorVersion, descriptorContainer.DescriptorVersion)
                self._update_mdib_version_group(reportNode)

            if len(waveformAge) > 0:
                minAge = min(waveformAge.values())
                maxAge = max(waveformAge.values())
                shall_log = self.waveform_time_warner.getOutOfDeterminationTimeLogState(minAge, maxAge, self.DETERMINATIONTIME_WARN_LIMIT)
                if shall_log != A_NO_LOG:
                    tmp = ', '.join('"{}":{:.3f}sec.'.format(k, v) for k,v in waveformAge.items())
                    if shall_log == A_OUT_OF_RANGE:
                        self._logger.warn('_onWaveformReport mdibVersion {}: age of samples outside limit of {} sec.: age={}!',
                                          mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
                    elif shall_log == A_STILL_OUT_OF_RANGE:
                        self._logger.warn('_onWaveformReport mdibVersion {}: age of samples still outside limit of {} sec.: age={}!',
                                          mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
                    elif shall_log == A_BACK_IN_RANGE:
                        self._logger.info('_onWaveformReport mdibVersion {}: age of samples back in limit of {} sec.: age={}',
                                          mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
            if LOG_WF_AGE_INTERVAL:
                now = time.time()
                if now - self._last_wf_age_log >= LOG_WF_AGE_INTERVAL:
                    age_data = self.get_wf_age_stdev()
                    self._logger.info('waveform mean age={:.1f}ms., stdev={:.2f}ms. min={:.1f}ms., max={}',
                                      age_data.mean_age*1000., age_data.stdev*1000.,
                                      age_data.min_age*1000., age_data.max_age*1000.)
                    self._last_wf_age_log = now
        finally:
            self._updateStateObservables(waveformByHandle.values())

    def _onEpisodicContextReport(self, reportNode, is_buffered_report=False):
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        if not is_buffered_report and self._bufferNotification(reportNode, self._onEpisodicContextReport):
            return

        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onEpisodicContextReport', mdib_version):
            return
        
        contextByHandle = {}
        stateContainers = self._msgReader.readEpisodicContextReport(reportNode)
        try:
            with self.mdibLock:
                for sc in stateContainers:
                    try:
                        oldStateContainer = self.contextStates.handle.getOne(sc.Handle, allowNone=True)
                    except RuntimeError as ex:
                        self._logger.error('_onEpisodicContextReport, getOne on contextStates: {}', ex)
                        continue

                    if oldStateContainer is not None:
                        if self._hasNewStateUsableStateVersion(oldStateContainer, sc, 'EpisodicContextReport', is_buffered_report):
                            self._logger.info(
                                '_onEpisodicContextReport: updated context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                                sc.Handle, sc.descriptorHandle, sc.ContextAssociation, sc.Validator)
                            oldStateContainer.updateFromOtherContainer(sc)
                            self.contextStates.updateObject(oldStateContainer)
                            contextByHandle[oldStateContainer.Handle] = oldStateContainer
                    else:
                        self.contextStates.addObject(sc)
                        self._logger.info(
                            '_onEpisodicContextReport: new context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                            sc.Handle, sc.descriptorHandle, sc.ContextAssociation, sc.Validator)
                        contextByHandle[sc.Handle] = sc
                self._update_mdib_version_group(reportNode)
        finally:
            self._updateStateObservables(contextByHandle.values())

    def _onEpisodicComponentReport(self, reportNode, is_buffered_report=False):
        """The EpisodicComponentReport is sent if at least one property of at least one component state has changed 
        and SHOULD contain only the changed component states.
        Components are MDSs, VMDs, Channels. Not metrics and alarms
        """
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        if not is_buffered_report and self._bufferNotification(reportNode, self._onEpisodicComponentReport):
            return

        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onEpisodicComponentReport', mdib_version):
            return
        
        componentByHandle = {}
        statecontainers = self._msgReader.readEpisodicComponentReport(reportNode)
        try:
            with self.mdibLock:
                for sc in statecontainers:
                    desc_h = sc.descriptorHandle
                    if desc_h is None:
                        self._logger.error('_onEpisodicComponentReport: missing descriptor handle in {}!',
                                           lambda: etree_.tostring(sc.node))  # pylint: disable=cell-var-from-loop
                    else:
                        try:
                            oldStateContainer = self.states.descriptorHandle.getOne(desc_h, allowNone=True)
                        except RuntimeError  as ex:
                            self._logger.error('_onEpisodicComponentReport, getOne on states: {}', ex)
                            continue

                        if oldStateContainer is not None:
                            if self._hasNewStateUsableStateVersion(oldStateContainer, sc, 'EpisodicComponentReport', is_buffered_report):
                                self._logger.info(
                                    '_onEpisodicComponentReport: updated component state, handle="{}" DescriptorVersion={}',
                                    desc_h, sc.DescriptorVersion)
                                oldStateContainer.updateFromOtherContainer(sc)
                                self.states.updateObject(oldStateContainer)
                                componentByHandle[oldStateContainer.descriptorHandle] = oldStateContainer
                        else:
                            self.states.addObject(sc)
                            self._logger.info(
                                '_onEpisodicComponentReport: new component state handle = {} DescriptorVersion={}',
                                desc_h, sc.DescriptorVersion)
                            componentByHandle[sc.descriptorHandle] = sc
                self._update_mdib_version_group(reportNode)
        finally:
            self._updateStateObservables(componentByHandle.values())


    def _onDescriptionModificationReport(self, reportNode, is_buffered_report=False):
        """The DescriptionModificationReport is sent if at least one Descriptor has been created, updated or deleted during runtime.
        It consists of 1...n DescriptionModificationReportParts.
        """
        # copy reportNode, further processing might change report data. Avoid side effects
        reportNode = xmlparsing.copy_node_wo_parent(reportNode)
        if not is_buffered_report and self._bufferNotification(reportNode, self._onDescriptionModificationReport):
            return

        mdib_version = self._extract_mdib_version(reportNode)
        if not self._canAcceptMdibVersion('_onDescriptionModificationReport', mdib_version):
            return

        descriptions_lookup_list = self._msgReader.readDescriptionModificationReport(reportNode)
        with self.mdibLock:
            self._update_mdib_version_group(reportNode)
            for descriptions_lookup in descriptions_lookup_list:
                newDescriptorByHandle = {}
                updatedDescriptorByHandle = {}

                # -- new --
                newDescriptorContainers, new_stateContainers = descriptions_lookup[pmtypes.DescriptionModificationTypes.CREATE]
                for dc in newDescriptorContainers:
                    if dc.isContextDescriptor:
                        msg = f"Creation of AbstractContextDescriptor is not supported. Descriptors @Handle '{dc.handle}'."
                        raise RuntimeError(msg)
                    self.descriptions.addObject(dc)
                    self._logger.debug('_onDescriptionModificationReport: created description "{}" (parent="{}")',
                                      dc.handle, dc.parentHandle)
                    newDescriptorByHandle[dc.handle] = dc
                for sc in new_stateContainers:
                    if sc.isContextState:
                        msg = f"Creating a new AbstractContextState is not supported. Descriptors @Handle '{sc.descriptorHandle}'."
                        raise RuntimeError(msg)
                    self.states.addObject(sc)

                # -- deleted --
                deletedDescriptorContainers, stateContainers = descriptions_lookup[pmtypes.DescriptionModificationTypes.DELETE]
                for dc in deletedDescriptorContainers:
                    if dc.isContextDescriptor:
                        msg = f"Deletion of AbstractContextDescriptor is not supported. Descriptors @Handle '{dc.handle}'."
                        raise RuntimeError(msg)
                    self._logger.debug('_onDescriptionModificationReport: remove descriptor "{}" (parent="{}")',
                                      dc.handle, dc.parentHandle)
                    self.rmDescriptorHandleAll(dc.handle) # handling of self.deletedDescriptorByHandle inside called method

                # -- updated --
                updatedDescriptorContainers, stateContainers = descriptions_lookup[pmtypes.DescriptionModificationTypes.UPDATE]
                for dc in updatedDescriptorContainers:
                    if dc.isContextDescriptor:
                        msg = f"Update of AbstractContextDescriptor is not supported. Descriptors @Handle '{dc.handle}'."
                        raise RuntimeError(msg)
                    self._logger.info('_onDescriptionModificationReport: update descriptor "{}" (parent="{}")',
                                      dc.handle, dc.parentHandle)
                    container = self.descriptions.handle.getOne(dc.handle, allowNone=True)
                    if container is None:
                        msg = f"An unexpected DescriptionModificationReport was received. No known descriptor exists for handle '{dc.handle}'."
                        raise ValueError(msg)
                    else:
                        container.updateDescrFromNode(dc.node)
                    updatedDescriptorByHandle[dc.handle] = dc
                for sc in stateContainers:
                    if sc.isContextState:
                        msg = f"Update of AbstractContextState is not supported. State to update '{sc.descriptorHandle}'."
                        raise RuntimeError(msg)
                    oldstateContainer = self.states.descriptorHandle.getOne(sc.descriptorHandle, allowNone=True)
                    if oldstateContainer is None:
                        msg = f"An unexpected DescriptionModificationReport was received. No known state exists for handle '{sc.descriptorHandle}'."
                        raise RuntimeError(msg)
                    oldstateContainer.updateFromOtherContainer(sc)
                    self.states.updateObject(oldstateContainer)

                # write observables for every report part separately
                if newDescriptorByHandle:
                    self.newDescriptorByHandle = newDescriptorByHandle
                if updatedDescriptorByHandle:
                    self.updatedDescriptorByHandle = updatedDescriptorByHandle
                if new_stateContainers:
                    self._updateStateObservables(new_stateContainers)
                if stateContainers:
                    self._updateStateObservables(stateContainers)


    def _hasNewStateUsableStateVersion(self, oldStateContainer, newStateContainer, reportName, is_buffered_report):
        """
        compare state versions old vs new
        :param oldStateContainer:
        :param newStateContainer:
        :param reportName: used for logging and for the message of a raised ValueError
        :param is_buffered_report: True if the report was buffered until the initial mdib was available
        :return: True if new state is ok for mdib , otherwise False
        :raise ValueError: if @StateVersion of the received state is not the expected one
        """
        old_version = int(oldStateContainer.StateVersion)
        new_version = int(newStateContainer.StateVersion)
        diff = new_version - old_version

        # refer to BICEPS R0038: @StateVersion has to be incremented by one when the content of the pm:AbstractState
        # ELEMENT changed or an ATTRIBUTE of the pm:AbstractState ELEMENT is changed
        # refer to BICEPS documentation of episodic reports: msg:AbstractReport SHALL contain only pm:AbstractState
        # instances where at least one child ELEMENT or ATTRIBUTE have changed.
        # -> only @StateVersion incremented by one is allowed

        if diff == 1:
            return True

        elif diff > 1:
            msg = (f'{reportName}: missed {diff - 1} state version(s) of state "{oldStateContainer.descriptorHandle}" '
                   f'({oldStateContainer.__class__.__name__}): received @StateVersion {new_version}, '
                   f'expected {old_version + 1}')
            raise ValueError(msg)

        else:
            if is_buffered_report:
                # the report was received before the initial GetMdib response, which already contains this state
                # or a newer one => the state of the report can be ignored
                return False
            msg = (f'{reportName}: received unexpected @StateVersion {new_version} for state '
                   f'"{oldStateContainer.descriptorHandle}" ({oldStateContainer.__class__.__name__}), '
                   f'current @StateVersion is {old_version}')
            raise ValueError(msg)

    def mkProposedState(self, descriptorHandle, copyCurrentState=True, handle=None):
        """ Create a new state that can be used as proposed state in according operations.
        The new state is not part of mdib!

        :param descriptorHandle: the descriptor
        :param copyCurrentState: if True, all members of existing state will be copied to new state
        :param handle: if this is a multi state class, then this is the handle of the existing state that shall be used for copy.
        :return:
        """
        descr = self.descriptions.handle.getOne(descriptorHandle)
        new_state = self.mkStateContainerFromDescriptor(descr)
        if copyCurrentState:
            lookup = self.contextStates if new_state.isContextState else self.states
            if new_state.isMultiState:
                if handle is None:  # new state
                    return new_state
                else:
                    old_state = lookup.handle.getOne(handle)
            else:
                old_state = lookup.descriptorHandle.getOne(descriptorHandle)
            new_state.updateFromOtherContainer(old_state)
        return new_state

    def get_wf_age_stdev(self):
        means = []
        stdevs = []
        mins = []
        maxs = []
        for buf in self.rtBuffers.values():
            age_data = buf.get_age_stdev()
            means.append(age_data.mean_age)
            stdevs.append(age_data.stdev)
            mins.append(age_data.min_age)
            maxs.append(age_data.max_age)
        return _AgeData(mean(means), mean(stdevs), min(mins), max(maxs))
