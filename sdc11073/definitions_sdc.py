import os

from lxml import etree as etree_

from .definitions_base import BaseDefinitions
from . import pmtypes
from .mdib.descriptorcontainers import get_container_class as get_descriptor_container_class
from .mdib.statecontainers import get_container_class as get_state_container_class

schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')

# the following namespace definitions reflect the initial SDC standard.
# There might be changes or additions in the future, who knows...

_DPWS_SDCNamespace = 'http://standards.ieee.org/downloads/11073/11073-20701-2018'  # pylint: disable=invalid-name
_ActionsNamespace = _DPWS_SDCNamespace  # pylint: disable=invalid-name


class _SdcV1Actions:
    OperationInvokedReport = _ActionsNamespace + '/SetService/OperationInvokedReport'
    SetOperationInvokedReport = _ActionsNamespace + '/SetService/OperationInvokedReport'
    ContextOperationInvokedReport = _ActionsNamespace + '/ContextService/OperationInvokedReport'
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


class SchemaPathsSdc:
    MetaDataExchangeSchemaFile = os.path.join(schemaFolder, 'MetadataExchange.xsd')
    EventingSchemaFile = os.path.join(schemaFolder, 'eventing.xsd')
    SoapEnvelopeSchemaFile = os.path.join(schemaFolder, 'soap-envelope.xsd')
    WsAddrSchemaFile = os.path.join(schemaFolder, 'ws-addr.xsd')
    XMLSchemaFile = os.path.join(schemaFolder, 'xml.xsd')
    DPWSSchemaFile = os.path.join(schemaFolder, 'wsdd-dpws-1.1-schema-os.xsd')
    WSDiscoverySchemaFile = os.path.join(schemaFolder, 'wsdd-discovery-1.1-schema-os.xsd')
    WSDLSchemaFile = os.path.join(schemaFolder, 'wsdl.xsd')
    MessageModelSchemaFile = os.path.join(schemaFolder, 'BICEPS_MessageModel.xsd')
    ParticipantModelSchemaFile = os.path.join(schemaFolder, 'BICEPS_ParticipantModel.xsd')
    ExtensionPointSchemaFile = os.path.join(schemaFolder, 'ExtensionPoint.xsd')
    schema_location_lookup = {  # for schema resolver
        # eventing.xsd originally uses http://schemas.xmlsoap.org/ws/2004/08/addressing,
        # but DPWS overwrites the addressing standard of eventing with http://www.w3.org/2005/08/addressing!
        # In order to reflect this, eventing.xsd was patched so that it uses same namespace and schema location
        # dpws uses a schema location that does not match the namespace!
        'http://www.w3.org/2006/03/addressing/ws-addr.xsd': WsAddrSchemaFile,  # schema loc. used by dpws 1.1 schema
        'http://www.w3.org/2001/xml.xsd': XMLSchemaFile,
        'http://www.w3.org/2003/05/soap-envelope': SoapEnvelopeSchemaFile,
        'http://standards.ieee.org/downloads/11073/11073-10207-2017/BICEPS_ParticipantModel.xsd': ParticipantModelSchemaFile,
        'http://standards.ieee.org/downloads/11073/11073-10207-2017/BICEPS_MessageModel.xsd': MessageModelSchemaFile,
        'http://standards.ieee.org/downloads/11073/11073-10207-2017/ExtensionPoint.xsd': ExtensionPointSchemaFile,
        'http://schemas.xmlsoap.org/ws/2004/08/eventing': EventingSchemaFile,
        'http://schemas.xmlsoap.org/ws/2004/09/mex': MetaDataExchangeSchemaFile,
        'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01': DPWSSchemaFile,
        'http://docs.oasis-open.org/ws-dd/discovery/1.1/os/wsdd-discovery-1.1-schema-os.xsd': WSDiscoverySchemaFile,
        'http://schemas.xmlsoap.org/wsdl/': WSDLSchemaFile
    }


class SDC_v1_Definitions(BaseDefinitions):  # pylint: disable=invalid-name
    BICEPSNamespace_base = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/'
    BICEPSNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/SP'
    DPWS_SDCNamespace = _DPWS_SDCNamespace  # 'http://standards.ieee.org/downloads/11073/11073-20701-2018'
    MedicalDeviceTypeNamespace = 'http://standards.ieee.org/downloads/11073/11073-20702-2016'
    MessageModelNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/message'
    ParticipantModelNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/participant'
    ExtensionPointNamespace = 'http://standards.ieee.org/downloads/11073/11073-10207-2017/extension'
    MDPWSNameSpace = 'http://standards.ieee.org/downloads/11073/11073-20702-2016'
    MedicalDeviceType = etree_.QName(MedicalDeviceTypeNamespace, 'MedicalDevice')
    SDCDeviceType = etree_.QName(DPWS_SDCNamespace, 'SdcDevice')
    ActionsNamespace = DPWS_SDCNamespace
    PortTypeNamespace = DPWS_SDCNamespace

    MedicalDeviceTypesFilter = [BaseDefinitions.DpwsDeviceType, MedicalDeviceType]
    get_descriptor_container_class = get_descriptor_container_class
    get_state_container_class = get_state_container_class
    Actions = _SdcV1Actions
    SchemaFilePaths = SchemaPathsSdc
    pmtypes = pmtypes
