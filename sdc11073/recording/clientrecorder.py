import logging
from zipfile import ZipFile
from datetime import datetime
from os import makedirs, listdir
import os.path as osp
from logging.handlers import RotatingFileHandler
from lxml.etree import tostring
from .. import observableproperties as properties
from .. import loghelper
from ..mdib.clientmdib import ClientMdibContainer

try:
    from .. import pysdc_version
    version = pysdc_version.version
except ImportError:
    version = '0.0.0'

"""
Client recorder is listening to mdib changes and using pythons logging feature
is storing all the data in the txt format
"""


class ClientRecorder(object):
    def __init__(self, sdcClient, path, log=False, filename="client_recording", maxBytes=0, backupCount=0):
        """
        Initialize the recorder with path to save files.

        Requires sdcClient instance for event binding.
        Recordings are stored in the path specified, each recording is stored in the new
        directory identified by timestamp. 
        Log is a flag to set recorder in log mode (other mode is 4replay)
        Optional filename can be specified (WITHOUT EXTENSION) 
        Rotating file handler is used as a mechanism for avoiding too large files
        hence max bytes can be specified with backupCount, both necessary
        """
        self._sdcClient = sdcClient
        self._path = path
        self._filename = filename
        self._log = log
        self._maxBytes = maxBytes
        self._backupCount = backupCount
        self.currentRecordingPath = self._path
        self._recorder = logging.getLogger('recorder')
        self._recorder.setLevel(logging.DEBUG)
        self._logger = loghelper.getLoggerAdapter('sdc.client')

    def startRecording(self):
        """
        Starts new recording with current mdib state as a reference
        Note that each recording will be saved in a new directory
        """
        self.currentRecordingPath = osp.join(self._path, "rec{}".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S")))
        makedirs(self.currentRecordingPath)
        self._initFileHandler()
        self._recordInitialMdibState()
        self._bindToObservables()
        self._logger.info("Recording started into directory {}".format(self.currentRecordingPath))

    def stopRecording(self):
        self._unbindObservables()
        self._logger.info("Recording stopped")

        for handler in self._recorder.handlers:
            handler.close()

        self._recorder.handlers = []

    def _recordInitialMdibState(self):
        getService = self._sdcClient.client('Get')
        mdibNode = getService.getMdibNode()
        mdibContainer = ClientMdibContainer(self._sdcClient)
        self._recorder.debug("Recorderd with pysdc ver: {}".format(version))
        mdibString = mdibContainer.nodeToString(mdibNode, pretty_print=False).decode('utf-8').replace('\n', '').replace('\r', '')
        self._recorder.debug(mdibString)

    def _initFileHandler(self):
        if self._log:
            filename = osp.join(self.currentRecordingPath, self._filename + ".log")
            fileHandlerChannel = RotatingFileHandler(filename=filename, maxBytes=self._maxBytes, backupCount=self._backupCount, encoding='UTF-8')
        else:
            filename = osp.join(self.currentRecordingPath, self._filename + ".rec")
            fileHandlerChannel = RotatingFileHandler(filename=filename, encoding='UTF-8')

        formatter = logging.Formatter("%(created)s|%(message)s")
        fileHandlerChannel.setFormatter(formatter)
        self._recorder.addHandler(fileHandlerChannel)

    def _unbindObservables(self):
        properties.unbind(self._sdcClient, waveFormReport=self._onReportMessage,
                          episodicMetricReport=self._onReportMessage, episodicAlertReport=self._onReportMessage,
                          episodicContextReport=self._onReportMessage, episodicComponentReport=self._onReportMessage,
                          descriptionModificationReport=self._onReportMessage,
                          episodicOperationalStateReport=self._onReportMessage)

    def _bindToObservables(self):
        properties.bind(self._sdcClient, waveFormReport=self._onReportMessage)
        properties.bind(self._sdcClient, episodicMetricReport=self._onReportMessage)
        properties.bind(self._sdcClient, episodicAlertReport=self._onReportMessage)
        properties.bind(self._sdcClient, episodicContextReport=self._onReportMessage)
        properties.bind(self._sdcClient, episodicComponentReport=self._onReportMessage)
        properties.bind(self._sdcClient, descriptionModificationReport=self._onReportMessage)
        properties.bind(self._sdcClient, episodicOperationalStateReport=self._onReportMessage)

    def _onReportMessage(self, reportNode):
        # For now assume messages do not differ in structure
        self._recorder.debug(repr(tostring(reportNode).decode('utf-8')))

    def archive(self):
        """
        Call this method to archive the most recent recording
        """
        suffixStart = len(self._filename)
        suffixEnd = suffixStart + 4
        archiveFile = osp.join(self.currentRecordingPath, self._filename + ".zip")
        with ZipFile(archiveFile, 'w') as recarchive:
            for recfile in listdir(self.currentRecordingPath):
                if recfile.endswith(".rec", suffixStart, suffixEnd):
                    recarchive.write(osp.join(self.currentRecordingPath, recfile), recfile)
        self._logger.info("Archive file has been created {}".format(archiveFile))
