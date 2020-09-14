from __future__ import print_function
from __future__ import absolute_import

import traceback
import os
import shutil
import uuid
import sdc11073
import time
import lxml

from sdc11073 import namespaces
from sdc11073.location import SdcLocation
from sdc11073.mdib.descriptorcontainers import NumericMetricDescriptorContainer
from sdc11073.pmtypes import CodedValue
from sdc11073.pysoap.soapenvelope import DPWSThisModel, DPWSThisDevice
from sdc11073.sdcdevice import SdcDevice, waveforms
from tests.base_test import BaseTest
from sdc11073 import pmtypes


class Test_Client_recording(BaseTest):
    def setUp(self):
        super(Test_Client_recording, self).setUp()
        self.setUpCocoDraft10()
        self.cleanUpDirs = []

    def tearDown(self):
        super(Test_Client_recording, self).tearDown()
        self.stopDraft10()
        for testDir in self.cleanUpDirs:
            try:
                shutil.rmtree(testDir)
            except:
                time.sleep(5)
                try:
                    shutil.rmtree(testDir)
                except:
                    print (traceback.format_exc())

    def testRecording(self):
        testFile = "testFile"

        # create and start recorder
        rec = sdc11073.recording.ClientRecorder(self.sdcClientCocoFinal, ".", filename=testFile)
        rec.startRecording()
        self.cleanUpDirs.append(rec.currentRecordingPath)

        # make changes to the mdib
        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction(setDeterminationTime=False) as mgr:
            mst = mgr.getMetricState('0x34F00100')
            if mst.metricValue is None:
                mst.mkMetricValue()
            mst.metricValue.Value = 12
            mst.metricValue.Validity = 'Vld'
            mst.metricValue.DeterminationTime = time.time()
            metricDescriptor = mgr.getDescriptor('0x34F00100')
            metricDescriptor.DeterminationPeriod = 29.0
            numericMetricDescriptorContainer = NumericMetricDescriptorContainer(
                                                            nsmapper=self.sdcDeviceCoCoFinal.mdib.nsmapper,
                                                            nodeName=namespaces.domTag('Metric'),
                                                            handle="testHandle",
                                                            parentHandle='2.1.1.1' #"COCO_2827", #""COCO_3120",
                                                            )
            numericMetricDescriptorContainer.Type = CodedValue('11921', 'sys')
            numericMetricDescriptorContainer.Unit = CodedValue('11921', 'sys')
            numericMetricDescriptorContainer.Resolution=0.01
            mgr.createDescriptor(numericMetricDescriptorContainer)

        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction(setDeterminationTime=False) as mgr:
            metricState = mgr.getMetricState("testHandle")
            metricState.Validity = 'Vld'

        time.sleep(1)

        mdsHandle = self.sdcDeviceCoCoFinal.mdib.descriptions.NODETYPE.getOne(sdc11073.namespaces.domTag('MdsDescriptor')).handle
        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction() as mgr:
            tst = mgr.getComponentState(mdsHandle)
            tst.ActivationState = "StndBy"
        time.sleep(1)

        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction(setDeterminationTime=True) as mgr:
            acst = mgr.getAlertState('0xD3C00100')
            acst.Presence = False
        time.sleep(1)

        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction(setDeterminationTime=True) as mgr:
            asst = mgr.getAlertState('0xD3C00100.loc.Vis')#('AlertSignal_0xD3C00100_Aud')
            asst.ActivationState = 'On'
        time.sleep(1)

        patientDescriptorContainer = self.sdcDeviceCoCoFinal.mdib.descriptions.NODETYPE.getOne(sdc11073.namespaces.domTag('PatientContextDescriptor'))
        # create a patient locally on device, then test update from client
        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction() as mgr:
            st = mgr.getContextState(patientDescriptorContainer.handle)
            st.Givenname = 'Max123'
            st.Middlename = 'Willy'
            st.Birthname = 'Mustermann \n'
            st.Familyname = 'Musterfrau'
            st.Title = 'Rex'
            st.Sex = 'M'
            st.PatientType = pmtypes.PatientType.ADULT
            st.Height = sdc11073.pmtypes.Measurement(88.2, sdc11073.pmtypes.CodedValue('abc', 'def'))
            st.Weight = sdc11073.pmtypes.Measurement(68.2, sdc11073.pmtypes.CodedValue('abc'))
            st.Race = sdc11073.pmtypes.CodedValue('123', 'def')

        newLocation = SdcLocation(fac='tasdaklx', poc='CsadU1', bed='cocoDraft10Bed')
        self.sdcDeviceCoCoFinal.setLocation(newLocation, [])



        paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1,
                                                          sampleperiod=0.01)
        self.sdcDeviceCoCoFinal.mdib.registerWaveformGenerator('0x34F05500', paw)

        # record some waveforms and then stop
        for x in range(20):
            time.sleep(1)
            # make changes to the mdib
            with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction(setDeterminationTime=False) as mgr:
                mst = mgr.getMetricState('0x34F00100')
                mst.metricValue.Value = x

        with self.sdcDeviceCoCoFinal.mdib.mdibUpdateTransaction(setDeterminationTime=True) as mgr:
            asst = mgr.getAlertState('0xD3C00100.loc.Vis') #('AlertSignal_0xD3C00100_Aud')
            asst.ActivationState = 'Off'
        time.sleep(1)
        rec.stopRecording()
        # verify recording has stopped by monitoring the file size
        recordingFile = os.path.join(rec.currentRecordingPath, testFile)+".rec"
        currentSize = os.path.getsize(recordingFile)
        time.sleep(1)
        self.assertEqual(currentSize, os.path.getsize(recordingFile))
        #
        # verify contents have metric and real time updates
        with open(recordingFile, 'r') as f:
            version = f.readline()
            self.assertTrue("pysdc ver" in version)

            mdib = f.readline()
            self.assertTrue("GetMdibResponse" in mdib)

            for line in f:
                nodeString = line.split("|", 1)[1]
                if nodeString.startswith("u'"):
                    nodeString = nodeString[2:-2]
                else:
                    nodeString = nodeString[1:-2]
                node = lxml.etree.fromstring(nodeString)

                if node.tag == namespaces.msgTag('DescriptionModificationReport'):
                    val = node.xpath('//dom:MetricValue', namespaces=namespaces.nsmap)
                    self.assertEqual(val[0].attrib['Value'], '12')
                    f.close()
                    break

        #verify archive has been created
        rec.archive()
        expectedZipFile = os.path.join(rec.currentRecordingPath, testFile) + ".zip"
        self.assertTrue(os.path.exists(expectedZipFile))
        rec_file = os.path.join(rec.currentRecordingPath, testFile) + ".rec"
        player = sdc11073.recording.MdibPlayer()
        mdib = player.readRecording(rec_file)

        model = DPWSThisModel(manufacturer="Draeger",
                              manufacturerUrl="draeger.com",
                              modelName="testMOdel",
                              modelNumber="231412411241",
                              modelUrl="draeger.com/testMOdel",
                              presentationUrl="draeger.com/testMOdel")
        device = DPWSThisDevice(friendlyName="SuperDevice",
                                firmwareVersion="v1.23",
                                serialNumber="MISAD31245124")

        self._publishingDevice = SdcDevice(self.wsdiscovery, uuid.uuid1(),
                                           model, device, mdib)
        # commLogger = commlog.CommLogger(log_folder="testcomm",
        #                                 log_out=True,
        #                                 log_in=False,
        #                                 broadcastIpFilter=None)
        # commlog.defaultLogger = commLogger

        self._publishingDevice.startAll()
        loca = SdcLocation(fac='Noneas', poc='CU1', bed='cocoDraft6Bed')
        self._publishingDevice.setLocation(loca, [])
        player.play(self._publishingDevice, loop=True)
        time.sleep(40)
        player.stop()
