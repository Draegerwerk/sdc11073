import traceback
import time
from threading import Lock
from dataclasses import dataclass
from lxml import etree as etree_
from .. import observableproperties as properties
from .. import namespaces
from .. import pmtypes
from .. import multikey
from typing import Union

class RtSampleContainer(object):
    '''Contains a single Value'''
    def __init__(self, valueString, timestamp, validity, annotations = None):
        self.valueString = valueString
        self.value = float(valueString)
        self.observationTime = timestamp
        self.validity = validity
        self.annotations = [] if annotations is None else annotations
    
    @property
    def age(self):
        return time.time() - self.observationTime
    
    def __repr__(self):
        return 'RtSample value="{}" validity="{}" time={}'.format(self.valueString, self.validity, self.observationTime)


@dataclass
class MdibVersionGroup:
    mdib_version: int
    sequence_id: str
    instance_id: Union[int, None]

    def update_node(self, node):
        """sets attributes in Node"""
        node.set('MdibVersion', str(self.mdib_version))
        node.set('SequenceId', self.sequence_id)
        if self.instance_id is not None:
            node.set('InstanceId', str(self.instance_id))


class _MultikeyWithVersionLookup(multikey.MultiKeyLookup):
    '''
    This class keeps track of versions of removed objects
    '''
    def __init__(self):
        multikey.MultiKeyLookup.__init__(self)
        self.handle_version_lookup = dict()


    def removeObject(self, obj):
        if obj is not None:
            self._saveVersion(obj)
        multikey.MultiKeyLookup.removeObject(self, obj)


    def removeObjectNoLock(self, obj):
        if obj is not None:
            self._saveVersion(obj)
        multikey.MultiKeyLookup.removeObjectNoLock(self, obj)

    def removeObjectsNoLock(self, objs):
        for obj in objs:
            if obj is not None:
                self._saveVersion(obj)
        multikey.MultiKeyLookup.removeObjectsNoLock(self, objs)


class DescriptorsLookup(_MultikeyWithVersionLookup):
    ''' This class knows about the hierarchy of descriptors and keeps the order of objects '''

    def __init__(self):
        _MultikeyWithVersionLookup.__init__(self)
        self.addIndex('handle', multikey.UIndexDefinition(lambda obj: obj.handle))
        self.addIndex('parentHandle', multikey.IndexDefinition(lambda obj: obj.parentHandle))
        self.addIndex('nodeName', multikey.IndexDefinition(lambda obj: obj.nodeName))
        self.addIndex('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE))
        self.addIndex('ConditionSignaled', multikey.IndexDefinition(lambda obj: obj.ConditionSignaled, indexNoneValues=False))
        # an index to find all alert conditions for a metric (AlertCondition is the only class that has a
        # "Source" attribute, therefore this simple approach without type testing is sufficient):
        self.addIndex('Source', multikey.IndexDefinition1n(lambda obj: [s.text for s in obj.Source], indexNoneValues=False))

        # ToDo: really 3 indices for coding needed?
        self.addIndex('codingSystem', multikey.IndexDefinition(lambda obj: obj.codingSystem))
        self.addIndex('codeId', multikey.IndexDefinition(lambda obj: obj.codeId))
        self.addIndex('coding', multikey.IndexDefinition(lambda obj: obj.coding))


    def _saveVersion(self, obj):
        self.handle_version_lookup[obj.handle] = obj.DescriptorVersion

    def setVersion(self, obj, increment=True):
        version = self.handle_version_lookup.get(obj.handle)
        if version is not None:
            if increment:
                version +=1
            obj.DescriptorVersion = version

    def addObject(self, obj):
        with self._lock:
            self.addObjectNoLock(obj)

    def addObjectNoLock(self, obj):
        ''' appends obj to parent'''
        _MultikeyWithVersionLookup.addObjectNoLock(self, obj)
        parent = None if obj.parentHandle is None else self.handle.getOne(obj.parentHandle, allowNone=True)
        if parent is not None:
            parent.addChild(obj)

    def addObjects(self, objs):
        with self._lock:
            self.addObjectsNoLock(objs)

    def addObjectsNoLock(self, objs):
        for obj in objs:
            self.addObjectNoLock(obj)

    def removeObject(self, obj):
        keys = self._objectIDs.get(id(obj))
        if keys is None:
            return
        with self._lock:
            self.removeObjectNoLock(obj)

    def removeObjectNoLock(self, obj):
        _MultikeyWithVersionLookup.removeObjectNoLock(self, obj)
        parent = self.handle.getOne(obj.parentHandle, allowNone=True)
        if parent is not None:
            parent.rmChild(obj)

    def removeObjects(self, objs):
        with self._lock:
            self.removeObjectsNoLock(objs)

    def removeObjectsNoLock(self, objs):
        for obj in objs:
            self.removeObjectNoLock(obj)

    def replaceObject(self, newObj):
        with self._lock:
            self.replaceObjectNoLock(newObj)

    def replaceObjectNoLock(self, newObj):
        ''' remove existing descriptorContainer and add new one, but do not touch childlist of parent (that keeps order)'''
        origObj = self.handle.getOne(newObj.handle)
        self.removeObjectNoLock(origObj)
        self.addObjectNoLock(newObj)


class StatesLookup(_MultikeyWithVersionLookup):
    def _saveVersion(self, obj):
        self.handle_version_lookup[obj.descriptorHandle] = obj.StateVersion

    def setVersion(self, obj, increment=True):
        version = self.handle_version_lookup.get(obj.descriptorHandle)
        if version is not None:
            if increment:
                version +=1
            obj.StateVersion = version


class MultiStatesLookup(_MultikeyWithVersionLookup):
    def _saveVersion(self, obj):
        self.handle_version_lookup[obj.Handle] = obj.StateVersion

    def setVersion(self, obj, increment=True):
        version = self.handle_version_lookup.get(obj.Handle)
        if version is not None:
            if increment:
                version +=1
            obj.StateVersion = version


class MdibContainer(object):

    # these observables can be used to watch any change of data in the mdib. They contain lists of containers that were changed.
    # every transaction (devicemdib) or notification (client mdib) will report their changes here.
    metricsByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    waveformByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    alertByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    contextByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    componentByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    newDescriptorByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    updatedDescriptorByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    deletedDescriptorByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    operationByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    deletedStatesByHandle = properties.ObservableProperty(fireOnlyOnChangedValue=False) # is a result of deleted descriptors
    sequenceId = properties.ObservableProperty()
    instanceId = properties.ObservableProperty()

    def __init__(self, sdc_definitions):
        '''
        @param sdc_definitions: a class derived from Definitions_Base
        '''
        self.sdc_definitions = sdc_definitions
        self._logger = None # must to be instantiated by derived class
        self.nsmapper = namespaces.DocNamespaceHelper()  # default map, might be replaced with nsmap from xml file  
        self.mdibVersion = 0
        self.sequenceId = ''  # needs to be set to a reasonable value by derived class
        self.instanceId = None # None or an unsigned int
        self.log_prefix = ''
        
        self.descriptions = DescriptorsLookup()

        self.states = StatesLookup() #multikey.MultiKeyLookup()
        self.states.addIndex('descriptorHandle', multikey.UIndexDefinition(lambda obj: obj.descriptorHandle))
        self.states.addIndex('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE, indexNoneValues=False))

        self.contextStates = MultiStatesLookup() #multikey.MultiKeyLookup()

        # descriptorHandle index is NOT unique!
        # => multiple ContextStates refer to the same descriptor( history of locations)
        # 'handle' index can be unique, because we ignore None values 
        self.contextStates.addIndex('descriptorHandle', multikey.IndexDefinition(lambda obj: obj.descriptorHandle))
        self.contextStates.addIndex('handle', multikey.UIndexDefinition(lambda obj: obj.Handle, indexNoneValues=False))
        self.contextStates.addIndex('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE, indexNoneValues=False))
        self.mdibLock = Lock()
        

        self.mdStateVersion = 0
        self.mdDescriptionVersion = 0

    @property
    def logger(self):
        return self._logger

    @property
    def mdib_version_group(self):
        return MdibVersionGroup(self.mdibVersion, self.sequenceId, self.instanceId)

    def addDescriptionContainers(self, descriptionContainers):
        ''' init self.descriptions with provided descriptors
        @param descriptionContainers: a list od DescriptionStateContainer objects
        '''
        newDescriptorByHandle = {}
        with self.descriptions._lock: #pylint: disable=protected-access
            for d in descriptionContainers:
                self.descriptions.addObjectNoLock(d)
                newDescriptorByHandle[d.handle] = d

        # finally update observable property
        if newDescriptorByHandle:
            self.newDescriptorByHandle = newDescriptorByHandle


    def clearStates(self):
        '''removes all states and context states. '''
        with self.states._lock: #pylint: disable=protected-access
            self.states.clear()
            self.contextStates.clear()

        # clear also the observable properties
        self.metricsByHandle = None
        self.waveformByHandle = None
        self.alertByHandle = None
        self.contextByHandle = None
        self.componentByHandle = None
        self.operationByHandle = None


    def _updateStateObservables(self, statecontainer_list):
        metricsByHandle = {}
        waveformByHandle = {}
        alertByHandle = {}
        contextByHandle = {}
        componentByHandle = {}
        operationByHandle = {}
        for sc in statecontainer_list:
            # add state to the corresponding dictionary, depending on type
            if sc.isAlertState:
                alertByHandle[sc.descriptorHandle] = sc
            elif sc.isRealtimeSampleArrayMetricState:  # test for this class before AbstractMetricStateContainer!!
                waveformByHandle[sc.descriptorHandle] = sc
            elif sc.isMetricState:
                metricsByHandle[sc.descriptorHandle] = sc
            elif sc.isComponentState:
                componentByHandle[sc.descriptorHandle] = sc
            elif sc.isOperationalState:
                operationByHandle[sc.descriptorHandle] = sc
            elif sc.isContextState:
                contextByHandle[sc.Handle] = sc
            elif sc.isSystemContextState or sc.isMultiState:
                pass   # ignoring for now
            elif sc.NODETYPE == namespaces.domTag('ScoState'): # special case Draft6 ScoState (is not a component state)
                pass  # this cannot be updated anyway over the network, but handle it here to avoid runtime error
            else:
                raise RuntimeError('handling of {} has been forgotten to implement!'.format(sc.__class__.__name__))

        #finally update observable properties
        if alertByHandle:
            self.alertByHandle = alertByHandle
        if waveformByHandle:
            self.waveformByHandle = waveformByHandle
        if metricsByHandle:
            self.metricsByHandle = metricsByHandle
        if componentByHandle:
            self.componentByHandle = componentByHandle
        if operationByHandle:
            self.operationByHandle = operationByHandle
        if contextByHandle:
            self.contextByHandle = contextByHandle


    def addStateContainers(self, stateContainers):
        '''Adds states to self.states and self.contextStates.
        @param stateContainers: a list of StateContainer objects.
        '''
        for sc in stateContainers:
            if sc.descriptorContainer is not None:
                self._logger.debug('addStateContainers: new state {}', sc)
            else:
                self._logger.warn('addStateContainers: new state {}, but has no descriptor!', sc)

            my_multikey = self.contextStates if sc.isContextState else self.states
            try:
                my_multikey.addObject(sc)
            except KeyError as ex:
                self._logger.error('addStateContainers: {}, keys={}; {}', ex,
                                   my_multikey.Handle.keys(), traceback.format_exc())

        # finally update observable properties
        self._updateStateObservables(stateContainers)

    setMdStates = addStateContainers # backwards compatibility


    def _reconstructMdDescription(self):
        '''build dom tree from current data
        @return: an etree_ node
        '''
        doc_nsmap = self.nsmapper.docNssmap
        rootContainers = self.descriptions.parentHandle.get(None) or []
        mdDescriptionNode = etree_.Element(namespaces.domTag('MdDescription'),
                                           attrib={'DescriptionVersion':str(self.mdDescriptionVersion)},
                                           nsmap=doc_nsmap)

        def connectDescriptors(parentContainer, parentNode):
            childContainers = parentContainer.getOrderedChildContainers()
            ret = parentContainer.connectChildContainers(parentNode, childContainers)
            # recursive call for children
            for childContainer, node in ret:
                connectDescriptors(childContainer, node)

        for rootContainer in rootContainers:
            n = rootContainer.mkDescriptorNode()
            mdDescriptionNode.append(n)
            connectDescriptors(rootContainer, n)
        return mdDescriptionNode


    def _reconstructMdib(self, addContextStates):
        '''build dom tree from current data
        @param addContextStates: bool
        @return: an etree_ node
        '''
        doc_nsmap = self.nsmapper.docNssmap
        mdibNode = etree_.Element(namespaces.msgTag('Mdib'), nsmap=doc_nsmap)
        self.mdib_version_group.update_node(mdibNode)
        mdDescriptionNode = self._reconstructMdDescription()
        mdibNode.append(mdDescriptionNode)

        # add a list of states
        mdStateNode = etree_.SubElement(mdibNode, namespaces.domTag('MdState'),
                                        attrib={'StateVersion':str(self.mdStateVersion)},
                                        nsmap=doc_nsmap)
        for stateContainer in self.states.objects:
            try:
                tmpNode = stateContainer.mkStateNode()
                mdStateNode.append(tmpNode)
            except RuntimeError:
                self._logger.error('State {} has no descriptorContainer', stateContainer.descriptorHandle)
        if addContextStates:
            for stateContainer in self.contextStates.objects:
                tmpNode = stateContainer.mkStateNode()
                mdStateNode.append(tmpNode)

        return mdibNode

    def reconstructMdDescription(self):
        '''build dom tree from current data
        @return: a tuple etree_ node, mdibVersion
        '''
        with self.mdibLock:
            node = self._reconstructMdDescription()
            return node, self.mdib_version_group

    def reconstructMdib(self):
        '''build dom tree from current data
        This method does not include context states!
        @return: an etree_ node
        '''
        with self.mdibLock:
            return self._reconstructMdib(addContextStates=False), self.mdib_version_group


    def reconstructMdibWithContextStates(self):
        ''' this method includes the context states in mdib tree.
        '''
        with self.mdibLock:
            return self._reconstructMdib(addContextStates=True), self.mdib_version_group


    def nodeToString(self, etree_node, pretty_print=False, xml_declaration=True, encoding='utf-8'):
        '''Special toString converter that replaces the internal normalized namespaces with the correct external namespaces.
        @return: a string
        '''
        mdibString = etree_.tostring(etree_node, pretty_print=pretty_print, xml_declaration=xml_declaration, encoding=encoding)
        return self.sdc_definitions.denormalizeXMLText(mdibString)


    def get_children_by_type_match(self, parent_handle: str, coding: Union[pmtypes.CodedValue, pmtypes.Coding]):
        """
        Returns all descriptors with matching parentHandle and Type.
        """
        all_children = self.descriptions.parentHandle.get(parent_handle)
        return [child for child in all_children if pmtypes.have_matching_codes(child.Type, coding)]

    def getDescriptorByCode(self, vmdCode: Union[pmtypes.CodedValue, pmtypes.Coding],
                            channelCode: Union[pmtypes.CodedValue, pmtypes.Coding],
                            metricCode: Union[pmtypes.CodedValue, pmtypes.Coding]):
        """ Find a descriptor by its Type hierarchy.
        :param vmdCode: a pmtypes.CodedValue or a pmtypes.Coding instance
        :param channelCode: a pmtypes.CodedValue or a pmtypes.Coding instance
        :param metricCode: a pmtypes.CodedValue or a pmtypes.Coding instance
        :return: None or a DescriptorContainer
        """
        all_vmds = self.descriptions.NODETYPE.get(namespaces.domTag('VmdDescriptor'))
        matching_vmd_handles = [vmd.Handle for vmd in all_vmds if pmtypes.have_matching_codes(vmd.Type, vmdCode)]

        matching_channels = []
        for handle in matching_vmd_handles:
            matching_channels.extend(self.get_children_by_type_match(handle, channelCode ))
        matching_channel_handles = [ch.Handle for ch in matching_channels]

        matching_leaf_descriptors = []
        for handle in matching_channel_handles:
            matching_leaf_descriptors.extend(self.get_children_by_type_match(handle, metricCode ))
        if len(matching_leaf_descriptors) == 0:
            return
        if len(matching_leaf_descriptors) > 1:
            raise RuntimeError('found multiple channel descriptors for vmd={} channel={} metric={}'.format(vmdCode, channelCode, metricCode))
        return matching_leaf_descriptors[0]

    getMetricDescriptorByCode = getDescriptorByCode  # backwards compatibility with previous name

    def getOperationsForMetric(self, vmdCode, channelCode, metricCode):
        """ This is the "correct" way to find an operation.
        Using well known handles is shaky, because they have no meaning and can change over time!
        @param vmdCode: a pmtypes.CodedValue or a pmtypes.Coding instance
        @param channelCode: a pmtypes.CodedValue or a pmtypes.Coding instance
        @param metricCode: a pmtypes.CodedValue or a pmtypes.Coding instance
        @return: a list of matching Operation Containers
        """
        descriptorContainer = self.getDescriptorByCode(vmdCode, channelCode, metricCode)
        return self.getOperationDescriptorsForDescriptorHandle(descriptorContainer.handle)


    def getOperationDescriptorsForDescriptorHandle(self, descriptorHandle, **additionalFilters):
        '''
        :param descriptorHandle: the handle for that operations shall be found
        :return: a list with operation descriptors that have descriptorHandle as OperationTarget. List can be empty
        :additionalFilters: optional filters for the key = name of member attribute, value = expected value
            example: NODETYPE=domTag('SetContextStateOperationDescriptor') filters for SetContextStateOperation descriptors
        '''
        allOperationContainers = self.getOperationDescriptors()
        myOperations = [opC for opC in allOperationContainers if opC.OperationTarget == descriptorHandle]
        for k, v in additionalFilters.items():
            myOperations = [op for op in myOperations if getattr(op, k) == v]
        return myOperations


    def getStateContainerClass(self, qNameType):
        '''
        @param qNameType: a QName instance
        '''
        cls = self.sdc_definitions.sc.getContainerClass(qNameType)
        if cls is None:
            self._logger.warn('No class for type={}; using AbstractStateContainer', str(qNameType))
            cls = self.sdc_definitions.sc.AbstractStateContainer
        return cls

    def getStateClsForDescriptor(self, descriptorContainer):
        stateClassQType = descriptorContainer.STATE_QNAME
        if stateClassQType is None:
            raise TypeError('No state association for {}'.format(descriptorContainer.__class__.__name__))
        return self.getStateContainerClass(stateClassQType)


    def mkStateContainerFromDescriptor(self, descriptorContainer):
        cls = self.getStateClsForDescriptor(descriptorContainer)
        if cls is None:
            raise TypeError('No state container class for descr={}, name={}, type={}'.format(descriptorContainer.__class__.__name__, descriptorContainer.nodeName, descriptorContainer.nodeType))
        return cls(self.nsmapper, descriptorContainer)


    def getOperationDescriptors(self):
        '''
        :return: a list of all operation descriptors
        '''
        result = []
        for nodeType in ('SetValueOperationDescriptor',
                         'SetStringOperationDescriptor',
                         'ActivateOperationDescriptor',
                         'SetContextStateOperationDescriptor',
                         'SetMetricStateOperationDescriptor',
                         'SetComponentStateOperationDescriptor',
                         'SetAlertStateOperationDescriptor'):
            result.extend(self.descriptions.NODETYPE.get(namespaces.domTag(nodeType), []))
        return result


    def getDescriptorContainerClass(self, qNameType):
        '''
        @param qNameType: a QName instance
        '''
        cls = self.sdc_definitions.dc.getContainerClass(qNameType)
        if cls is None:
            self._logger.warn('No class for type={}; using AbstractDescriptorContainer',
                              str(qNameType))
            raise RuntimeError('No class for type={}; using AbstractDescriptorContainer'.format(str(qNameType)))
        return cls


    def selectDescriptors(self, *codings):
        ''' Returns all descriptor containers that match a path defined by list of codings.
        example: 
        ['70041'] returns all containers that have CodedValue = 70041
        ['70041', '69650'] : returns all descriptors with CodedValue= 69650 and parent descriptor CodedValue = 70041
        ['70041', '69650', '69651'] : returns all descriptors with CodedValue= 69651 and parent descriptor CodedValue = 69650 and parent's parent descriptor CodedValue = 70041
        It is not necessary that path starts at the top of an mds, it can start anywhere.  
        '''
        selectedObjects = None
        for coding in codings:
            
            if selectedObjects is None:
                selectedObjects = self.descriptions.objects # initially all objects
            else:
                # get all children of selected objects
                allhandles = [o.handle for o in selectedObjects]
                selectedObjects = []
                for h in allhandles:
                    selectedObjects.extend(self.descriptions.parentHandle.get(h, []))
                
            # normalize coding
            if isinstance(coding, str):
                coding = pmtypes.CodedValue(coding, pmtypes.DefaultCodingSystem).coding
            elif hasattr(coding, 'coding'):
                coding=coding.coding
                
            if coding is not None:
                # apply filter
                tmpObjects = [o for o in selectedObjects if o.coding == coding ]
                selectedObjects = tmpObjects
        return selectedObjects                    


    def getAllDescriptorsInSubTree(self, descriptorContainer, depthFirst=True, includeRoot=True):
        ''' walks the tree below descriptorContainer.
        :param descriptorContainer:
        :param depthFirst: determines order of returned list. DepthFirst=True has all leaves on top, otherwise at the end.
        :param includeRoot: if True descriptorContainer itself is also part of returned list
        :return: a list of DescriptorContainer objects
        '''
        result = []
        def _getchildren(parent):
            childContainers = self.descriptions.parentHandle.get(parent.handle, list())
            if not depthFirst:
                result.extend(childContainers)
            for ch in childContainers:
                _getchildren(ch)
            if depthFirst:
                result.extend(childContainers)
        if includeRoot and not depthFirst:
            result.append(descriptorContainer)
        _getchildren(descriptorContainer)
        if includeRoot and depthFirst:
            result.append(descriptorContainer)
        return result


    def _rmDescriptorsAndStates(self, descriptorContainers):
        ''' recursive delete of a descriptor and all children and all related states'''
        deletedDescriptorByHandle = {}
        deletedStatesByHandle = {}
        for descriptorContainer in descriptorContainers:
            self._logger.debug('rm Descriptor node {} handle {}',
                               descriptorContainer.nodeName, descriptorContainer.handle)
            self.descriptions.removeObject(descriptorContainer)
            deletedDescriptorByHandle[descriptorContainer.handle] = descriptorContainer
            for m_key in (self.states, self.contextStates):
                stateContainers = m_key.descriptorHandle.get(descriptorContainer.handle)
                if stateContainers is not None:
                    # make a copy, otherwise removeObjects will manipulate same list in place
                    stateContainers = stateContainers[:]
                    self._logger.debug('rm {} states(s) associated to descriptor {} ',
                                      len(stateContainers), descriptorContainer.handle)
                    m_key.removeObjects(stateContainers)
                    deletedStatesByHandle[descriptorContainer.handle] = stateContainers

        if deletedDescriptorByHandle:
            self.deletedDescriptorByHandle = deletedDescriptorByHandle
        if deletedStatesByHandle:
            self.deletedStatesByHandle = deletedStatesByHandle


    def rmDescriptorHandleAll(self, handle):
        descriptorContainer = self.descriptions.handle.getOne(handle, allowNone=True)
        if descriptorContainer is not None:
            allDescriptors = self.getAllDescriptorsInSubTree(descriptorContainer)
            self._rmDescriptorsAndStates(allDescriptors)
