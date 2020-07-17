import threading
import traceback
from copy import deepcopy
from lxml.etree import fromstring, tostring
from sdc11073.mdib.statecontainers import *
from sdc11073.namespaces import domTag
from sdc11073.namespaces import Prefix_Namespace as Prefix
from ..pmtypes import DescriptionModificationTypes as ModificationTypes
from ..mdib.devicemdib import DeviceMdibContainer
from ..mdib.msgreader import MessageReader
from .. import loghelper

_MSG = Prefix.MSG.namespace
DESCRIPTOR_MODIFICATION = "{%s}DescriptionModificationReport" % _MSG
EPISODIC_METRIC = "{%s}EpisodicMetricReport" % _MSG
WAVEFORM_STREAM = "{%s}WaveformStream" % _MSG
WAVEFORM_STREAM_REPORT = "{%s}WaveformStreamReport" % _MSG
EPISODIC_CONTEXT = "{%s}EpisodicContextReport" % _MSG
EPISODIC_ALERT = "{%s}EpisodicAlertReport" % _MSG
EPISODIC_COMPONENT = "{%s}EpisodicComponentReport" % _MSG
EPISODIC_OPERATIONAL_STATE = "{%s}EpisodicOperationalStateReport" % _MSG

class PlayerThread(threading.Thread):
    """Main player thread that is looping through rec file
    and pushing descriptor updates to the mdib"""

    def __init__(self, device, filename, timestamp, loop):
        threading.Thread.__init__(self)
        self._device = device
        self._file = filename
        self._loop = loop
        self._isLooping = False
        self._timestamp = timestamp
        self._logger = loghelper.getLoggerAdapter('sdc.device.player')
        self._reader = MessageReader(self._device.mdib)
        self.stop = False
        self._mdsContainer = self._device.mdib.reconstructMdibWithContextStates()

    def run(self):
        """Main player thread: it will skip version line and mdib line
        then will replay each change and reset to very beginning of the file
        if looping is enabled then will remove all the changes from mdib
        and add stored default version"""
        try:
            with open(self._file, 'r') as recording:
                while not self.stop:
                    next(recording)  # skip version
                    next(recording)  # skip mdib

                    for line in recording:
                        if self.stop:
                            break
                        self._updateState(line)
                    recording.seek(0)

                    if not self._loop:
                        break
                    self._isLooping = True
                    self._removeMdsDescriptor()
                    self._addMdsDescriptor()
        except Exception:
            self._logger.error('Unhandled Exception at thread runtime. Thread will abort! {}'.format(traceback.format_exc()))
            raise

    def _removeMdsDescriptor(self):
        handle = self._device.mdib.descriptions.NODETYPE.getOne(domTag('MdsDescriptor')).handle #node.attrib['Handle']
        with self._device.mdib.mdibUpdateTransaction() as mgr:
            mgr.removeDescriptor(handle)

    def _addMdsDescriptor(self):
        with self._device.mdib.mdibUpdateTransaction() as mgr:
            tempMds = deepcopy(self._mdsContainer)
            self._device.mdib.addMdsNode(tempMds)

    def _updateState(self, line):
        timestamp, node = line.split('|', 1)
        timestamp = float(timestamp)

        if node.startswith("u'"):
            nodeString = node[2:-2]
        else:
            nodeString = node[1:-2]

        nodeTree = fromstring(nodeString)
        # sleep the difference if its bigger than 0
        if (timestamp - self._timestamp) > 0.0:
            time.sleep(timestamp - self._timestamp)
        self._timestamp = timestamp

        # for each type of report separate logic is required
        if nodeTree.tag == DESCRIPTOR_MODIFICATION:
            self._onDescriptionModificationReport(nodeTree)
        elif nodeTree.tag == EPISODIC_METRIC:
            self._onEpisodicMetricReport(nodeTree)
        elif nodeTree.tag in (WAVEFORM_STREAM, WAVEFORM_STREAM_REPORT):
            self._onWaveformStream(nodeTree)
        elif nodeTree.tag == EPISODIC_CONTEXT:
            self._onEpisodicContextReport(nodeTree)
        elif nodeTree.tag == EPISODIC_ALERT:
            self._onEpisodicAlertReport(nodeTree)
        elif nodeTree.tag == EPISODIC_COMPONENT:
            self._onEpisodicComponentReport(nodeTree)
        elif nodeTree.tag == EPISODIC_OPERATIONAL_STATE:
            self._onEpisodicOperationalStateReport(nodeTree)
        else:
            self._logger.exception("Unknown report {}".format(tostring(nodeTree)))

    def _onEpisodicMetricReport(self, reportNode):
        self._logger.debug("Replaying episodic metric report")
        try:
            states = self._reader.readEpisodicMetricReport(reportNode)

            with self._device.mdib.mdibUpdateTransaction() as mgr:
                for stateContainer in states:
                    try:
                        oldStateContainer = mgr.getMetricState(stateContainer.descriptorHandle)
                        oldStateContainer.updateFromOtherContainer(stateContainer, skippedProperties=['StateVersion'])
                    except RuntimeError as ex:
                        self._logger.debug(
                            "Could not replay state {} with {}".format(stateContainer.descriptorHandle, ex))

        except Exception as e:
            self._logger.error("Failed to replay episodic metric report {}".format(e))

    def _onDescriptionModificationReport(self, reportNode):
        self._logger.debug("Replaying descriptor modification report")
        try:
            descriptions_lookup_list = self._reader.readDescriptionModificationReport(reportNode)

            for descriptions_lookup in descriptions_lookup_list:
                # create new descriptors
                newDescriptorContainers, newStateContainers = descriptions_lookup[ModificationTypes.CREATE]
                with self._device.mdib.mdibUpdateTransaction() as mgr:
                    for dc in newDescriptorContainers:
                        mgr.createDescriptor(dc)

                for sc in newStateContainers:
                    # determine multikey
                    if sc.NODETYPE.localname.endswith('ContextState'):
                        multikey = self._device.mdib.contextStates
                    else:
                        multikey = self._device.mdib.states
                    multikey.addObject(sc)

                # update descriptors
                upDescriptorContainers, upStateContainers = descriptions_lookup[ModificationTypes.UPDATE]
                with self._device.mdib.mdibUpdateTransaction() as mgr:
                    for dc in upDescriptorContainers:
                        descriptor = mgr.getDescriptor(dc.handle)
                        descriptor.updateDescrFromNode(dc.node)

                for sc in upStateContainers:
                    # determine multikey
                    if sc.NODETYPE.localname.endswith('ContextState'):
                        multikey = self._device.mdib.contextStates
                        oldstateContainer = multikey.handle.getOne(sc.handle, allowNone=True)
                    else:
                        multikey = self._device.mdib.states
                        oldstateContainer = multikey.descriptorHandle.getOne(sc.descriptorHandle, allowNone=True)
                    if oldstateContainer is not None:
                        oldstateContainer.updateFromOtherContainer(sc, skippedProperties=['StateVersion'])
                        multikey.updateObject(oldstateContainer)

                # remove descriptors
                delDescriptorContainers, _ = descriptions_lookup[ModificationTypes.DELETE]
                with self._device.mdib.mdibUpdateTransaction() as mgr:
                    for dc in delDescriptorContainers:
                        mgr.removeDescriptor(dc.handle)
        except Exception as e:
            self._logger.error("Failed to replay descriptor modification report {}".format(e))

    def _onWaveformStream(self, reportNode):
        self._logger.debug("Replaying waveform report")
        try:
            states = self._reader.readWaveformReport(reportNode)

            with self._device.mdib.mdibUpdateTransaction() as mgr:
                for stateContainer in states:
                    try:
                        oldStateContainer = mgr.getRealTimeSampleArrayMetricState(stateContainer.descriptorHandle)
                        stateVersion = oldStateContainer.StateVersion
                        oldStateContainer.updateFromOtherContainer(stateContainer, skippedProperties=['StateVersion'])
                        if oldStateContainer.metricValue is not None:
                            oldStateContainer.metricValue.DeterminationTime = time.time()
                        oldStateContainer.StateVersion = stateVersion
                    except RuntimeError as ex:
                        self._logger.debug(
                            "Could not replay state {} with {}".format(stateContainer.descriptorHandle, ex))
        except Exception as e:
            self._logger.error("Failed to replay waveform report {}".format(e))

    def _onEpisodicContextReport(self, reportNode):
        self._logger.debug("Replaying episodic context report")
        try:
            states = self._reader.readEpisodicContextReport(reportNode)

            with self._device.mdib.mdibUpdateTransaction() as mgr:
                for stateContainer in states:
                    oldStateContainer = None
                    try:
                        oldStateContainer = mgr.getContextState(stateContainer.descriptorHandle, stateContainer.Handle)
                        oldStateContainer.updateFromOtherContainer(stateContainer, skippedProperties=['StateVersion'])
                    except RuntimeError as ex:
                        self._logger.info(
                            "Could not find state {} with {}".format(stateContainer.descriptorHandle, ex))

        except Exception as e:
            self._logger.error("Failed to replay episodic context report {}".format(e))

    def _onEpisodicAlertReport(self, reportNode):
        self._logger.debug("Replaying episodic alert report")
        try:
            states = self._reader.readEpisodicAlertReport(reportNode)

            with self._device.mdib.mdibUpdateTransaction() as mgr:
                for stateContainer in states:
                    try:
                        oldStateContainer = mgr.getAlertState(stateContainer.descriptorHandle)
                        oldStateContainer.updateFromOtherContainer(stateContainer, skippedProperties=['StateVersion'])
                    except RuntimeError as ex:
                        self._logger.debug(
                            "Could not replay state {} with {}".format(stateContainer.descriptorHandle, ex))
        except Exception as e:
            self._logger.error("Failed to replay episodic alert report {}".format(e))

    def _onEpisodicComponentReport(self, reportNode):
        self._logger.debug("Replaying episodic component report")
        try:
            states = self._reader.readEpisodicComponentReport(reportNode)

            with self._device.mdib.mdibUpdateTransaction() as mgr:
                for stateContainer in states:
                    try:
                        oldStateContainer = mgr.getComponentState(stateContainer.descriptorHandle)
                        oldStateContainer.updateFromOtherContainer(stateContainer, skippedProperties=['StateVersion'])
                    except RuntimeError as ex:
                        self._logger.debug(
                            "Could not replay state {} with {}".format(stateContainer.descriptorHandle, ex))
        except Exception as e:
            self._logger.error("Failed to replay episodic component report {}".format(e))

    def _onEpisodicOperationalStateReport(self, reportNode):
        self._logger.debug("Replaying episodic operational state report")
        try:
            states = self._reader.readOperationalStateReport(reportNode)

            with self._device.mdib.mdibUpdateTransaction() as mgr:
                for stateContainer in states:
                    try:
                        oldStateContainer = mgr.getOperationalState(stateContainer.descriptorHandle)
                        oldStateContainer.updateFromOtherContainer(stateContainer)
                    except RuntimeError as ex:
                        self._logger.debug(
                            "Could not replay state {} with {}".format(stateContainer.descriptorHandle, ex))
        except Exception as e:
            self._logger.error("Failed to replay episodic operational state report {}".format(e))


class MdibPlayer(object):
    """
        Class that will read mdib from recording
        Create a player thread and replay the changes as sdc messages
    """

    def __init__(self):
        self._timestamp = time.time()
        self._logger = loghelper.getLoggerAdapter('sdc.device.player')
        self._file = None
        self._playerThread = None

    def readRecording(self, fileName):
        """
        :param fileName: filename of the recording as there could be many
        :return:
        """
        if fileName.endswith("rec"):
            self._file = fileName
            with open(self._file, 'r') as recording:
                recording.readline() # skip versioning
                line = recording.readline()
                self._timestamp, mdib = line.split('|', 1)
                self._timestamp = float(self._timestamp)

            self._logger.info("Read file {}".format(fileName))
            self._logger.debug("Mdib file {}".format(mdib))

            if sys.version_info >= (3, 0):
                return DeviceMdibContainer.fromString(mdib.encode('utf-8'))
            return DeviceMdibContainer.fromString(mdib)
        else:
            self._logger.error("Wrong file provided. Rec expected, but received {}".format(fileName))
            raise TypeError("Not a rec file")

    def play(self, device, loop=False):
        if self._file is not None:
            self._playerThread = PlayerThread(device, self._file, self._timestamp, loop)
            self._playerThread.start()
            self._logger.info("Player thread started")
        else:
            raise ValueError("No rec file provided")

    def stop(self):
        if self._playerThread is not None:
            self._playerThread.stop = True
            self._logger.info("Player thread stopped")
