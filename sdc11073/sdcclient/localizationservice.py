
from lxml import etree as etree_
from .hostedservice import HostedServiceClient
from ..namespaces import msgTag
from ..pmtypes import LocalizedText

class LocalizationServiceClient(HostedServiceClient):

    def _getLocalizedTextResponse(self, refs=None, version=None, langs=None, textWidths=None, numberOfLines=None, request_manipulator=None):
        '''

        :param refs: a list of strings or None
        :param version: an unsigned integer or None
        :param langs: a list of strings or None
        :param textWidths: a list of strings or None (each string one of xs, s, m, l, xs, xxs)
        :param numberOfLines: a list of unsigned integers or None
        :param request_manipulator:
        :return: a list of LocalizedText objects
        '''
        envelope = self._msg_factory.mk_getlocalizedtext_envelope(self.endpoint_reference.address, self.porttype,
                                                                 refs, version, langs, textWidths, numberOfLines)
        resultSoapEnvelope = self._callGetMethod(envelope, 'GetLocalizedText',
                                                 request_manipulator=request_manipulator)
        return resultSoapEnvelope


    def getLocalizedTextNode(self, refs=None, version=None, langs=None, textWidths=None, numberOfLines=None, request_manipulator=None):
        return self._getLocalizedTextResponse(refs, version, langs, textWidths, numberOfLines, request_manipulator).msgNode

    def getLocalizedTexts(self, refs=None, version=None, langs=None, textWidths=None, numberOfLines=None, request_manipulator=None):
        result = []
        responseNode = self._getLocalizedTextResponse(refs, version, langs, textWidths, numberOfLines, request_manipulator).msgNode
        if responseNode is not None:
            for element in responseNode:
                lt = LocalizedText.from_node(element)
                result.append(lt)
        return result

    def _get_supported_languages(self, request_manipulator=None):
        envelope = self._msg_factory.mk_getsupportedlanguages_envelope(
            self.endpoint_reference.address, self.porttype)
        return self._callGetMethod(envelope, 'GetSupportedLanguages', request_manipulator=request_manipulator)

    def getSupportedLanguages(self, request_manipulator=None):
        resultSoapEnvelope = self._get_supported_languages(request_manipulator)
        result = []
        for element in resultSoapEnvelope.msgNode:
            result.append(str(element.text))
        return result

    def getSupportedLanguagesNodes(self, request_manipulator=None):
        resultSoapEnvelope = self._get_supported_languages(request_manipulator)
        return resultSoapEnvelope.msgNode

