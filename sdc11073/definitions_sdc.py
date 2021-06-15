import os
from lxml import etree as etree_
from .namespaces import Prefix_Namespace as Prefix
from .definitions_base import SchemaResolverBase
from .definitions_base import BaseDefinitions
from .mdib import descriptorcontainers as dc_final
from .mdib import statecontainers as sc_final
from .pysoap.msgfactory import SoapMessageFactory
from .pysoap.msgreader import MessageReader
from .sdcdevice.sdcservicesimpl import GetService, SetService, StateEventService,  ContainmentTreeService
from .sdcdevice.sdcservicesimpl import ContextService, WaveformService, DescriptionEventService
from .sdcdevice.localizationservice import LocalizationService
from .sdcdevice.sdc_handlers import SdcHandler_Full
from .sdcdevice.sco import getOperationClass, ScoOperationsRegistry
from .sdcdevice.subscriptionmgr import SubscriptionsManager

from .sdcclient.subscription import SOAPNotificationsHandler
from .sdcclient.hostedservice import GetServiceClient, SetServiceClient, StateEventClient
from .sdcclient.hostedservice import CTreeServiceClient, DescriptionEventClient, ContextServiceClient, WaveformClient
from .sdcclient.localizationservice import LocalizationServiceClient
from .sdcclient.subscription import SubscriptionClient, NotificationsReceiverDispatcherThread
from .sdcclient.operations import OperationsManager


schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')

class _SchemaResolver(SchemaResolverBase):
    lookup_ext = {
            'http://standards.ieee.org/downloads/11073/11073-10207-2017/BICEPS_ParticipantModel.xsd': 'ParticipantModelSchemaFile',
            'http://standards.ieee.org/downloads/11073/11073-10207-2017/BICEPS_MessageModel.xsd': 'MessageModelSchemaFile',
            'http://standards.ieee.org/downloads/11073/11073-10207-2017/ExtensionPoint.xsd': 'ExtensionPointSchemaFile', }

# the following namespace definitions reflect the initial SDC standard.
# There might be changes or additions in the future, who knows...
#

_DPWS_SDCNamespace = 'http://standards.ieee.org/downloads/11073/11073-20701-2018'
_ActionsNamespace = _DPWS_SDCNamespace

class _SDC_v1_Actions(object):
    OperationInvokedReport = _ActionsNamespace + '/SetService/OperationInvokedReport'
    SetOperationInvokedReport = _ActionsNamespace + '/SetService/OperationInvokedReport'
    ContextOperationInvokedReport =  _ActionsNamespace + '/ContextService/OperationInvokedReport'
    EpisodicContextReport = _ActionsNamespace + '/ContextService/EpisodicContextReport'
    EpisodicMetricReport = _ActionsNamespace + '/StateEventService/EpisodicMetricReport'
    EpisodicOperationalStateReport = _ActionsNamespace + '/StateEventService/EpisodicOperationalStateReport'
    EpisodicAlertReport = _ActionsNamespace + '/StateEventService/EpisodicAlertReport'
    EpisodicComponentReport = _ActionsNamespace + '/StateEventService/EpisodicComponentReport'
    PeriodicContextReport = _ActionsNamespace + '/ContextService/PeriodicContextReport'
    PeriodicMetricReport = _ActionsNamespace + '/StateEventService/PeriodicMetricReport'
    PeriodicOperationalStateReport = _ActionsNamespace + '/StateEventService/PeriodicOperationalStateReport'
    PeriodicAlertReport = _ActionsNamespace + '/StateEventService/PeriodicAlertReport'
    PeriodicComponentReport = _ActionsNamespace + '/StateEventService/PeriodicComponentReport'
    SystemErrorReport = _ActionsNamespace + '/StateEventService/SystemErrorReport'
    Waveform = _ActionsNamespace + '/WaveformService/WaveformStream'
    DescriptionModificationReport = _ActionsNamespace + '/DescriptionEventService/DescriptionModificationReport'
    GetMdib = _ActionsNamespace + '/GetService/GetMdib'
    GetMdibResponse = _ActionsNamespace + '/GetService/GetMdibResponse'
    GetMdState = _ActionsNamespace + '/GetService/GetMdState'
    GetMdStateResponse = _ActionsNamespace + '/GetService/GetMdStateResponse'
    GetMdDescription = _ActionsNamespace + '/GetService/GetMdDescription'
    GetMdDescriptionResponse = _ActionsNamespace + '/GetService/GetMdDescriptionResponse'
    GetContainmentTree = _ActionsNamespace + '/GetService/GetContainmentTree'
    GetContainmentTreeResponse = _ActionsNamespace + '/GetService/GetContainmentTreeResponse'
    GetContextStates = _ActionsNamespace + '/ContextService/GetContextStates'
    GetContextStatesResponse = _ActionsNamespace + '/ContextService/GetContextStatesResponse'
    GetContextStatesByIdentification = _ActionsNamespace + '/ContextService/GetContextStatesByIdentification'
    GetContextStatesByIdentificationResponse = _ActionsNamespace + '/ContextService/GetContextStatesByIdentificationResponse'
    SetContextState = _ActionsNamespace + '/ContextService/SetContextState'
    SetContextStateResponse = _ActionsNamespace + '/ContextService/SetContextStateResponse'
    GetSupportedLanguages = _ActionsNamespace + '/LocalizationService/GetSupportedLanguages'
    GetSupportedLanguagesResponse = _ActionsNamespace + '/LocalizationService/GetSupportedLanguagesResponse'
    GetLocalizedText = _ActionsNamespace + '/LocalizationService/GetLocalizedText'
    GetLocalizedTextResponse = _ActionsNamespace + '/LocalizationService/GetLocalizedTextResponse'
    Activate = _ActionsNamespace + '/SetService/Activate'
    ActivateResponse = _ActionsNamespace + '/SetService/ActivateResponse'
    SetString = _ActionsNamespace + '/SetService/SetString'
    SetStringResponse = _ActionsNamespace + '/SetService/SetStringResponse'
    SetValue = _ActionsNamespace + '/SetService/SetValue'
    SetValueResponse = _ActionsNamespace + '/SetService/SetValueResponse'
    SetAlertState = _ActionsNamespace + '/SetService/SetAlertState'
    SetAlertStateResponse = _ActionsNamespace + '/SetService/SetAlertStateResponse'
    SetMetricState = _ActionsNamespace + '/SetService/SetMetricState'
    SetMetricStateResponse = _ActionsNamespace + '/SetService/SetMetricStateResponse'
    SetComponentState = _ActionsNamespace + '/SetService/SetComponentState'
    SetComponentStateResponse = _ActionsNamespace + '/SetService/SetComponentStateResponse'
    GetDescriptor = _ActionsNamespace + '/ContainmentTreeService/GetDescriptor'
    GetDescriptorResponse = _ActionsNamespace + '/ContainmentTreeService/GetDescriptorResponse'
    GetContainmentTree = _ActionsNamespace + '/ContainmentTreeService/GetContainmentTree'
    GetContainmentTreeResponse = _ActionsNamespace + '/ContainmentTreeService/GetContainmentTreeResponse'
    SubscriptionEnd = Prefix.WSE.namespace + '/SubscriptionEnd'


"""Dependency injection: This dictionary defines which component implementations the sdc client will use. """
DefaultSdcClientComponents = {
    'MsgFactoryClass':  SoapMessageFactory,
    'MsgReaderClass': MessageReader,
    'NotificationsReceiverClass': NotificationsReceiverDispatcherThread,
    'NotificationsHandlerClass': SOAPNotificationsHandler,
    'SubscriptionManagerClass': SubscriptionClient,
    'OperationsManagerClass': OperationsManager,
    'ServiceHandlers': {'ContainmentTreeService': CTreeServiceClient,
                         'GetService': GetServiceClient,
                         'StateEventService': StateEventClient,
                         'ContextService': ContextServiceClient,
                         'WaveformService': WaveformClient,
                         'SetService': SetServiceClient,
                         'DescriptionEventService': DescriptionEventClient,
                         'LocalizationService': LocalizationServiceClient,
                         }
}


"""Dependency injection: This dictionary defines which component implementations the sdc device will use. """
DefaultSdcDeviceComponents = {
    'MsgFactoryClass': SoapMessageFactory,
    'MsgReaderClass': MessageReader,
    'SdcDeviceHandlerClass': SdcHandler_Full,
    'OperationsFactory': getOperationClass,
    'ScoOperationsRegistryClass': ScoOperationsRegistry,
    'SubscriptionsManagerClass': SubscriptionsManager,
    'ServiceHandlers': {'ContainmentTreeService': ContainmentTreeService,
                        'GetService': GetService,
                        'StateEventService': StateEventService,
                        'ContextService': ContextService,
                        'WaveformService': WaveformService,
                        'SetService': SetService,
                        'DescriptionEventService': DescriptionEventService,
                        'LocalizationService': LocalizationService,
                        }
}


class SDC_v1_Definitions(BaseDefinitions):
    BICEPSNamespace_base = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/'
    BICEPSNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/SP'
    DPWS_SDCNamespace = _DPWS_SDCNamespace#'http://standards.ieee.org/downloads/11073/11073-20701-2018'
    MedicalDeviceTypeNamespace = 'http://standards.ieee.org/downloads/11073/11073-20702-2016'
    MessageModelNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/message'
    ParticipantModelNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/participant'
    ExtensionPointNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/extension'
    MDPWSNameSpace = 'http://standards.ieee.org/downloads/11073/11073-20702-2016' # this name changed between WPF and SDC! IEEE correction
    MedicalDeviceType = etree_.QName(MedicalDeviceTypeNamespace, 'MedicalDevice')
    SDCDeviceType = etree_.QName(DPWS_SDCNamespace, 'SdcDevice')
    MessageModelSchemaFile = os.path.join(schemaFolder, 'BICEPS_MessageModel.xsd')
    ParticipantModelSchemaFile = os.path.join(schemaFolder, 'BICEPS_ParticipantModel.xsd')
    ExtensionPointSchemaFile = os.path.join(schemaFolder, 'ExtensionPoint.xsd')
    ActionsNamespace = DPWS_SDCNamespace
    PortTypeNamespace = DPWS_SDCNamespace

    MedicalDeviceTypesFilter = [BaseDefinitions.DpwsDeviceType, MedicalDeviceType]
    sc = sc_final
    dc = dc_final
    Actions = _SDC_v1_Actions
    DefaultSdcDeviceComponents = DefaultSdcDeviceComponents
    DefaultSdcClientComponents = DefaultSdcClientComponents


SDC_v1_Definitions.schemaResolver = _SchemaResolver(SDC_v1_Definitions)
