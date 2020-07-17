from lxml import etree as etree_
import copy
from .. import namespaces
from .. import pmtypes


class MdibStructureError(Exception):
    pass


class MessageReader(object):
    ''' This class does all the conversions from DOM trees (body of SOAP messages) to MDIB objects.'''
    def __init__(self, mdib):
        self._mdib = mdib
        self._logger = mdib.logger
        self._logPrefix = ''


    @staticmethod
    def getMdibRootNode(sdc_definitions, xml_text):
        '''
        Creates a normalized and validated elementtree from xml_text.
        normalizing means that draft6 or final BICEPS namespaces are replaced by an standardized internal namespace.
        :param xml_text: xml document
        :return: elementtree node of the root element
        '''
        xml_text = sdc_definitions.normalizeXMLText(xml_text)
        parser = etree_.ETCompatXMLParser(remove_comments=True, remove_blank_text=True)
        root = etree_.fromstring(xml_text, parser=parser, base_url=None)
        if root.tag != namespaces.msgTag('GetMdibResponse'):
            getmdibResponseNodes = root.xpath('//msg:GetMdibResponse', namespaces=namespaces.nsmap)
            if len(getmdibResponseNodes) != 1:
                raise ValueError('provided dom does not contain a msg:GetMdibResponse node!')
            else:
                root = getmdibResponseNodes[0]
        return root


    def readMdDescription(self, node):
        '''
        Parses a GetMdDescriptionResponse or the MdDescription part of GetMdibResponse
        :param node: An etree node
        :return: a list of DescriptorContainer objects, sorted depth last
        '''
        descriptions = []
        mdDescriptionNodes = node.xpath('//dom:MdDescription', namespaces=namespaces.nsmap)
        if not mdDescriptionNodes:
            raise ValueError('no MdDescription node found in tree')
        else:
            mdDescriptionNode = mdDescriptionNodes[0]

        def addChildren(parentNode):
            p_handle = parentNode.get('Handle')
            for childNode in parentNode:
                if childNode.get('Handle') is not None:
                    container = self.mkDescriptorContainerFromNode(childNode, p_handle)
                    descriptions.append(container)
                    addChildren(childNode)

        # iterate over tree, collect all handles of vmds, channels and metric descriptors
        allmds = mdDescriptionNode.findall(namespaces.domTag('Mds'))
        for mdsNode in allmds:
            mds = self.mkDescriptorContainerFromNode(mdsNode, None)
            descriptions.append(mds)
            addChildren(mdsNode)
        return descriptions


    def readMdState(self, node, additionalDescriptorContainers=None):
        '''
        Parses a GetMdStateResponse or the MdState part of GetMdibResponse
        :param node: A node that contains MdState nodes
        :param additionalDescriptorContainers: a list of descriptor containers that can also be used for state creation
                (typically used if descriptors and states are created in the same transaction. In that case the descriptors are not yet part of mdib.)
        :return: a list of state containers
        '''
        stateContainers = []
        mdStateNodes = node.xpath('//dom:MdState', namespaces=namespaces.nsmap)
        if mdStateNodes:
            allstates = mdStateNodes[0].findall(namespaces.domTag('State'))
            for state in allstates:
                try:
                    stateContainers.append(self.mkStateContainerFromNode(state, additionalDescriptorContainers=additionalDescriptorContainers))
                except MdibStructureError as ex:
                    self._logger.error('{}readMdState: cannot create: {}', self._logPrefix, ex)
        return stateContainers


    def readContextState(self, getContextStatesResponseNode):
        ''' Creates Context State Containers from dom tree.
        @param getContextstatesResponseNode: node "getContextStatesResponse" of getContextStates.
        :param additionalDescriptorContainers: a list of descriptor containers that can also be used for state creation
                (typically used if descriptors and states are created in the same transaction. In that case the descriptors are not yet part of mdib.)
        @return: a list of state containers
        '''
        states = []
        contextStateNodes = list(getContextStatesResponseNode) # list of msg:ContextStatenodes
        for contextStateNode in contextStateNodes:
            # hard remame to dom:State
            contextStateNode.tag = namespaces.domTag('State')
            try:
                stateContainer = self.mkStateContainerFromNode(contextStateNode)
                states.append(stateContainer)
            except MdibStructureError as ex:
                self._logger.error('{}readContextState: cannot create: {}', self._logPrefix, ex)
        return states


    def mkDescriptorContainerFromNode(self, node, parentHandle):
        '''

        :param node: a descriptor node
        :param parentHandle: the handle of the parent
        :return: a DescriptorContainer object representing the content of node
        '''
        nodeType = node.get(namespaces.QN_TYPE)
        if nodeType is not None:
            nodeType = namespaces.txt2QName(nodeType, node.nsmap)
        else:
            nodeType = etree_.QName(node.tag)
        cls = self._mdib.getDescriptorContainerClass(nodeType)
        return cls.fromNode(self._mdib.nsmapper, node, parentHandle)


    def mkStateContainerFromNode(self, node, forcedType=None, additionalDescriptorContainers = None):
        '''
        @param node: a etree node
        @param forcedType: if given, the QName that shall be used for class instantiation instead of the data in node
        '''
        if forcedType is not None:
            nodeType = forcedType
        else:
            nodeType = node.get(namespaces.QN_TYPE)
            if nodeType is not None:
                nodeType = namespaces.txt2QName(nodeType, node.nsmap)

        descriptorHandle = node.get('DescriptorHandle')
        descriptorContainer = self._mdib.descriptions.handle.getOne(descriptorHandle, allowNone=True)
        if descriptorContainer is None:
            if additionalDescriptorContainers is not None:
                correspondingDescriptors = [ d for d in additionalDescriptorContainers if d.handle == descriptorHandle]
            else:
                correspondingDescriptors = None
            if correspondingDescriptors is None or len(correspondingDescriptors) == 0:
                raise MdibStructureError(
                    'new state {}: descriptor with handle "{}" does not exist!'.format(nodeType.localname,
                                                                                       descriptorHandle))
            else:
                descriptorContainer = correspondingDescriptors[0]
        cls = self._mdib.getStateContainerClass(nodeType)
        if node.tag != namespaces.domTag('State'):
            node = copy.copy(node)  # make a copy, do not modify the original report
            node.tag = namespaces.domTag('State')
        return cls(self._mdib.nsmapper, descriptorContainer, node)


    def _mkStateContainersFromReportPart(self, reportPartNode):
        containers = []
        for childNode in reportPartNode:
            desc_h = childNode.get('DescriptorHandle')
            if desc_h is None:
                self._logger.error('{}_onEpisodicComponentReport: missing descriptor handle in {}!',
                                   self._logPrefix, lambda:etree_.tostring(childNode))  #pylint: disable=cell-var-from-loop
            else:
                containers.append(self.mkStateContainerFromNode(childNode))
        return containers


    def readWaveformReport(self, reportNode):
        '''
        Parses a waveform report
        :param reportNode: A waveform report etree
        :return: a list of StateContainer objects
        '''
        states = []
        allSampleArrays = list(reportNode)
        for sampleArray in allSampleArrays:
            if sampleArray.tag.endswith('State'): # ignore everything else, e.g. Extension
                sc = self.mkStateContainerFromNode(sampleArray, namespaces.domTag('RealTimeSampleArrayMetricState'))
                states.append(sc)
        return states


    def readEpisodicMetricReport(self, reportNode):
        '''
        Parses an episodic metric report
        :param reportNode:  An episodic metric report etree
        :return: a list of StateContainer objects
        '''
        states = []
        reportPartNodes = reportNode.xpath('msg:ReportPart', namespaces=namespaces.nsmap)
        for reportPartNode in reportPartNodes:
            states.extend(self._mkStateContainersFromReportPart(reportPartNode))
        return states


    def readEpisodicAlertReport(self, reportNode):
        '''
        Parses an episodic alert report
        :param reportNode:  An episodic alert report etree
        :return: a list of StateContainer objects
        '''
        states = []
        allAlerts = reportNode.xpath('msg:ReportPart/msg:AlertState', namespaces=namespaces.nsmap)
        for alert in allAlerts:
            sc = self.mkStateContainerFromNode(alert)
            states.append(sc)
        return states


    def readOperationalStateReport(self, reportNode):
        '''
        Parses an operational state report
        :param reportNode:  An operational state report etree
        :return: a list of StateContainer objects
        '''
        states = []
        allOperationStateNodes = reportNode.xpath('msg:ReportPart/msg:OperationState', namespaces=namespaces.nsmap)
        for opStateNode in allOperationStateNodes:
            sc = self.mkStateContainerFromNode(opStateNode)
            states.append(sc)
        return states


    def readEpisodicContextReport(self, reportNode):
        '''
        Parses an episodic context report
        :param reportNode:  An episodic context report etree
        :return: a list of StateContainer objects
        '''
        states = []
        reportPartNodes = reportNode.xpath('msg:ReportPart', namespaces=namespaces.nsmap)
        for reportPartNode in reportPartNodes:
            sc = self._mkStateContainersFromReportPart(reportPartNode)
            states.extend(sc)
        return states


    def readEpisodicComponentReport(self, reportNode):
        '''
        Parses an episodic component report
        :param reportNode:  An episodic component report etree
        :return: a list of StateContainer objects
        '''
        states = []
        componentStateNodes = reportNode.xpath('msg:ReportPart/msg:ComponentState', namespaces=namespaces.nsmap)
        for componentState in componentStateNodes:
            sc = self.mkStateContainerFromNode(componentState)
            states.append(sc)
        return states


    def readDescriptionModificationReport(self, reportNode):
        '''
        Parses a description modification report
        :param reportNode:  A description modification report etree
        :return: a list of DescriptorContainer objects
        '''
        descriptors_list = []
        reportParts = list(reportNode) # list of msg:ReportPart nodes
        for reportPart in reportParts:
            descriptors = {pmtypes.DescriptionModificationTypes.UPDATE: ([], []),
                           pmtypes.DescriptionModificationTypes.CREATE: ([], []),
                           pmtypes.DescriptionModificationTypes.DELETE: ([], []),
                           }
            descriptors_list.append(descriptors)
            parentDescriptor = reportPart.get('ParentDescriptor')
            modificationType = reportPart.get('ModificationType', 'Upt')  # implied Value is 'Upt'
            descriptorNodes = reportPart.findall(namespaces.msgTag('Descriptor'))
            for descriptorNode in descriptorNodes:
                dc = self.mkDescriptorContainerFromNode(descriptorNode, parentDescriptor)
                descriptors[modificationType][0].append(dc)
            stateNodes = reportPart.findall(namespaces.msgTag('State'))
            for stateNode in stateNodes:
                sc = self.mkStateContainerFromNode(stateNode, additionalDescriptorContainers=descriptors[modificationType][0])
                descriptors[modificationType][1].append(sc)
        return descriptors_list
