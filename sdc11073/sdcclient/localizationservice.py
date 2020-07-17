
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
        requestparams = []
        if refs is not None:
            for r in refs:
                node = etree_.Element(msgTag('Ref'))
                node.text = r
                requestparams.append(node)
        if version is not None:
            node = etree_.Element(msgTag('Version'))
            node.text = str(version)
            requestparams.append(node)
        if langs is not None:
            for l in langs:
                node = etree_.Element(msgTag('Lang'))
                node.text = l
                requestparams.append(node)
        if textWidths is not None:
            for tw in textWidths:
                node = etree_.Element(msgTag('TextWidth'))
                node.text = tw
                requestparams.append(node)
        if numberOfLines is not None:
            for nol in numberOfLines:
                node = etree_.Element(msgTag('NumberOfLines'))
                node.text = nol
                requestparams.append(node)

        resultSoapEnvelope = self._callGetMethod('GetLocalizedText', params=requestparams,
                                                 request_manipulator=request_manipulator)
        return resultSoapEnvelope

    def getLocalizedTextNode(self, refs=None, version=None, langs=None, textWidths=None, numberOfLines=None, request_manipulator=None):
        return self._getLocalizedTextResponse(refs, version, langs, textWidths, numberOfLines, request_manipulator).msgNode

    def getLocalizedTexts(self, refs=None, version=None, langs=None, textWidths=None, numberOfLines=None, request_manipulator=None):
        result = []
        responseNode = self._getLocalizedTextResponse(refs, version, langs, textWidths, numberOfLines, request_manipulator).msgNode
        if responseNode is not None:
            for element in responseNode:
                lt = LocalizedText.fromNode(element)
                result.append(lt)
        return result

    def getSupportedLanguages(self, request_manipulator=None):
        resultSoapEnvelope = self._callGetMethod('GetSupportedLanguages', request_manipulator=request_manipulator)
        result = []
        for element in resultSoapEnvelope.msgNode:
            result.append(str(element.text))
        return result

    def getSupportedLanguagesNodes(self, request_manipulator=None):
        resultSoapEnvelope = self._callGetMethod('GetSupportedLanguages', request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

