import unittest
from lxml import etree as etree_
import sdc11073.mdib.descriptorcontainers as descriptorcontainers
import sdc11073.namespaces as namespaces
import sdc11073.pmtypes as pmtypes



class TestDescriptorContainers(unittest.TestCase):

    def setUp(self):
        self.nsmapper = namespaces.DocNamespaceHelper()


    def test_AbstractDescriptorContainer(self):
        dc = descriptorcontainers.AbstractDescriptorContainer(nsmapper=self.nsmapper,
                                                              nodeName='MyDescriptor',
                                                              handle='123',
                                                              parentHandle='456',
                                                              )

        self.assertEqual(dc.DescriptorVersion, 0)
        self.assertEqual(dc.SafetyClassification, 'Inf')
        self.assertEqual(dc.getActualValue('SafetyClassification'), None)
        self.assertEqual(dc.Type, None)

        #test creation from node
        node = dc.mkNode()
        dc2 = descriptorcontainers.AbstractDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                        node=node,
                                                                        parentHandle='467')
        self.assertEqual(dc2.DescriptorVersion, 0)
        self.assertEqual(dc2.SafetyClassification, 'Inf')
        self.assertEqual(dc.Type, None)
        self.assertEqual(dc.ext_Extension, None)

        #test update from node
        dc.DescriptorVersion = 42
        dc.SafetyClassification = 'MedA'
        dc.Type = pmtypes.CodedValue('abc', 'def')

        dc.ext_Extension = etree_.Element(namespaces.extTag('Extension'))
        etree_.SubElement(dc.ext_Extension, 'foo', attrib={'someattr':'somevalue'})
        etree_.SubElement(dc.ext_Extension, 'bar', attrib={'anotherattr':'differentvalue'})
        node = dc.mkNode()
        dc2.updateDescrFromNode(node)

        self.assertEqual(dc2.DescriptorVersion, 42)
        self.assertEqual(dc2.SafetyClassification, 'MedA')
        self.assertEqual(dc2.Type, dc.Type)
        self.assertEqual(dc.codeId, 'abc')
        self.assertEqual(dc.codingSystem, 'def')
        self.assertEqual(dc2.ext_Extension.tag, namespaces.extTag('Extension'))
        self.assertEqual(len(dc2.ext_Extension), 2)


    def test_AbstractMetricDescriptorContainer(self):     
        dc = descriptorcontainers.AbstractMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                    nodeName='MyDescriptor',
                                                                    handle='123',
                                                                    parentHandle='456',
                                                                    )
        self.assertEqual(dc.MetricAvailability, 'Cont') # the default value
        self.assertEqual(dc.MetricCategory, 'Unspec') # the default value
        self.assertEqual(dc.DeterminationPeriod, None)
        self.assertEqual(dc.MaxMeasurementTime, None)
        self.assertEqual(dc.MaxDelayTime, None)

        #test creation from node
        node = dc.mkNode()
        dc2 = descriptorcontainers.AbstractMetricDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                        node=node,
                                                                        parentHandle='467')
        self.assertEqual(dc2.MetricAvailability, 'Cont')
        self.assertEqual(dc2.MetricCategory, 'Unspec')
        self.assertEqual(dc2.DeterminationPeriod, None)
        self.assertEqual(dc2.MaxMeasurementTime, None)
        self.assertEqual(dc2.MaxDelayTime, None)

        #test update from node
        dc.MetricAvailability = 'Avail'
        dc.MetricCategory = 'Msmnt'
        
        dc.DeterminationPeriod = 3.5
        dc.MaxMeasurementTime = 2.1
        dc.MaxDelayTime = 4
        dc.Unit = pmtypes.CodedValue('abc', 'def')
        dc.BodySite.append( pmtypes.CodedValue('ABC', 'DEF'))
        dc.BodySite.append( pmtypes.CodedValue('GHI', 'JKL'))
        node = dc.mkNode()
        dc2.updateDescrFromNode(node)

        self.assertEqual(dc2.MetricAvailability, 'Avail')
        self.assertEqual(dc2.MetricCategory, 'Msmnt')
        self.assertEqual(dc2.DeterminationPeriod, 3.5)
        self.assertEqual(dc2.MaxMeasurementTime, 2.1)
        self.assertEqual(dc2.MaxDelayTime, 4)
        self.assertEqual(dc2.Unit, dc.Unit)
        self.assertEqual(dc2.BodySite, dc.BodySite)
        self.assertEqual(dc2.BodySite, [pmtypes.CodedValue('ABC', 'DEF'), pmtypes.CodedValue('GHI', 'JKL')])


    def test_NumericMetricDescriptorContainer(self):
        dc = descriptorcontainers.NumericMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                   nodeName='MyDescriptor',
                                                                   handle='123',
                                                                   parentHandle='456',
                                                                   )
        self.assertEqual(dc.Resolution, None)
        self.assertEqual(dc.AveragingPeriod, None)
    
    
    def test_EnumStringMetricDescriptorContainer(self):
        dc = descriptorcontainers.EnumStringMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                      nodeName=namespaces.domTag('MyDescriptor'),
                                                                      handle='123',
                                                                      parentHandle='456',
                                                                      )
        dc.AllowedValue = [pmtypes.AllowedValue('abc')]
        
        node = dc.mkNode()
        print (etree_.tostring(node, pretty_print=True))
        dc2 = descriptorcontainers.EnumStringMetricDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                                node=node,
                                                                                parentHandle='467')
        self.assertEqual(dc.AllowedValue, dc2.AllowedValue)
        
        
    def _cmp_AlertConditionDescriptorContainer(self, dc, dc2):
        self.assertEqual(dc.Source, dc2.Source)
        self.assertEqual(dc.CauseInfo, dc2.CauseInfo)
        self.assertEqual(dc.Kind, dc2.Kind)
        self.assertEqual(dc.Priority, dc2.Priority)

    def test_AlertConditionDescriptorContainer(self):    
        dc = descriptorcontainers.AlertConditionDescriptorContainer(nsmapper=self.nsmapper,
                                                                    nodeName=namespaces.domTag('MyDescriptor'),
                                                                    handle='123',
                                                                    parentHandle='456',
                                                                    )
        # create copy with default values
        node = dc.mkNode()
        dc2 = descriptorcontainers.AlertConditionDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                              node=node,
                                                                              parentHandle='467')
        self._cmp_AlertConditionDescriptorContainer(dc, dc2)

        # set values, test updateFromNode
        dc.Source = [pmtypes.ElementWithTextOnly('A'),pmtypes.ElementWithTextOnly('B')]
        dc.Cause = [pmtypes.CauseInfo(remedyInfo=pmtypes.RemedyInfo([pmtypes.LocalizedText('abc'), pmtypes.LocalizedText('def')]),
                                      descriptions=[pmtypes.LocalizedText('descr1'), pmtypes.LocalizedText('descr2')]),
                    pmtypes.CauseInfo(remedyInfo=pmtypes.RemedyInfo([pmtypes.LocalizedText('123'), pmtypes.LocalizedText('456')]),
                                      descriptions=[pmtypes.LocalizedText('descr1'), pmtypes.LocalizedText('descr2')])
                    ]
        dc.Kind = 'cont'
        dc.Priority = 'High'
        node = dc.mkNode()
        dc2.updateDescrFromNode(node)
        self._cmp_AlertConditionDescriptorContainer(dc, dc2)
        
        # create copy with values set
        dc2 = descriptorcontainers.AlertConditionDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                              node=node,
                                                                              parentHandle='467')
        self._cmp_AlertConditionDescriptorContainer(dc, dc2)


    def _cmp_LimitAlertConditionDescriptorContainer(self, dc, dc2):
        self.assertEqual(dc.MaxLimits, dc2.MaxLimits)
        self.assertEqual(dc.AutoLimitSupported, dc2.AutoLimitSupported)
        self._cmp_AlertConditionDescriptorContainer(dc, dc2)

    def test_LimitAlertConditionDescriptorContainer(self):
        dc = descriptorcontainers.LimitAlertConditionDescriptorContainer(nsmapper=self.nsmapper,
                                                                         nodeName=namespaces.domTag('MyDescriptor'),
                                                                         handle='123',
                                                                         parentHandle='456',
                                                                         )
        # create copy with default values
        node = dc.mkNode()
        dc2 = descriptorcontainers.LimitAlertConditionDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                                   node=node,
                                                                                   parentHandle='467')
        self._cmp_LimitAlertConditionDescriptorContainer(dc, dc2)
        
        # set values, test updateFromNode
        dc.MaxLimits = pmtypes.Range(lower=0, upper=100, stepWidth=1, relativeAccuracy=0.1, absoluteAccuracy=0.2)
        dc.AutoLimitSupported = True
        node = dc.mkNode()
        dc2.updateDescrFromNode(node)
        self._cmp_LimitAlertConditionDescriptorContainer(dc, dc2)
        
        # create copy with values set
        dc2 = descriptorcontainers.LimitAlertConditionDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                                   node=node,
                                                                                   parentHandle='467')
        self._cmp_LimitAlertConditionDescriptorContainer(dc, dc2)



    def test_ActivateOperationDescriptorContainer(self):
        def _cmp_ActivateOperationDescriptorContainer(_dc, _dc2):
            self.assertEqual(_dc.diff(_dc2), [])
            self.assertEqual(_dc.Argument, _dc2.Argument)
#            self.assertEqual(_dc.ActivationDuration, _dc2.ActivationDuration)
            self.assertEqual(_dc.Retriggerable, _dc2.Retriggerable)

        dc = descriptorcontainers.ActivateOperationDescriptorContainer(nsmapper=self.nsmapper,
                                                                              nodeName=namespaces.domTag(
                                                                                  'MyDescriptor'),
                                                                              handle='123',
                                                                              parentHandle='456',
                                                                              )
        # create copy with default values
        node = dc.mkNode()
        dc2 = descriptorcontainers.ActivateOperationDescriptorContainer.fromNode(nsmapper=self.nsmapper,
                                                                                        node=node,
                                                                                        parentHandle='467')
        _cmp_ActivateOperationDescriptorContainer(dc, dc2)

        dc.Argument = [pmtypes.Argument(argName=pmtypes.CodedValue('abc', 'def'), arg=namespaces.domTag('blubb'))]
        node = dc.mkNode()
        dc2.updateDescrFromNode(node)
        _cmp_ActivateOperationDescriptorContainer(dc, dc2)

    def test_ClockDescriptorContainer_Final(self):
        self._test_ClockDescriptorContainer(cls=descriptorcontainers.ClockDescriptorContainer)


    def _test_ClockDescriptorContainer(self, cls):
        dc = cls(nsmapper=self.nsmapper,
                 nodeName=namespaces.domTag('MyDescriptor'),
                 handle='123',
                 parentHandle='456',
                 )
        # create copy with default values
        node = dc.mkNode()
        dc2 = cls.fromNode(nsmapper=self.nsmapper,
                           node=node,
                           parentHandle='467')
        self.assertEqual(dc.TimeProtocol, dc2.TimeProtocol)
        self.assertEqual(dc.Resolution, dc2.Resolution)

        dc.TimeProtocol = [pmtypes.CodedValue('abc', 'def'), pmtypes.CodedValue('123', '456')]
        dc.Resolution = 3.14
        node = dc.mkNode()
        dc2.updateDescrFromNode(node)
        self.assertEqual(dc.TimeProtocol, dc2.TimeProtocol)
        self.assertEqual(dc.Resolution, dc2.Resolution)



def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestDescriptorContainers)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
