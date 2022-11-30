from collections import defaultdict
from lxml import etree as etree_
from .. import pysoap
from ..namespaces import Prefix_Namespace as Prefix
from ..namespaces import msgTag, dpwsTag, nsmap

from .sdcservicesimpl import DPWSPortTypeImpl
from .sdcservicesimpl import WSDLMessageDescription, WSDLOperationBinding
from .sdcservicesimpl import _wsdl_ns, _mkWsdlTwowayOperation

_msg = Prefix.MSG.prefix

def _tw2i(textwidth_string):
    ''' text width to int'''
    lookup = {'xs': 0, 's':1, 'm':2, 'l':3, 'xl':4, 'xxl':5, None: 999}
    return lookup[textwidth_string]

def _calcNoL(text):
    # definition of a line in Participant Model:
    # ...a line is defined as the content of the text from either the beginning of the text or the beginning of
    # a previous line until the next occurance of period mark, question mark, exclamation mark, or paragraph.
    # TBD: naive approach?
    return len(text.split('\n'))


def _textWidth_filter(localized_texts, width):
    i_width = _tw2i(width)
    candidates = [v for v in localized_texts if _tw2i(v.TextWidth) <= i_width]
    if candidates:
        candidates.sort(key=lambda obj:  _tw2i(obj.TextWidth) or -1)
    return candidates

def _n_o_l_filter(localized_texts, n_o_l):
    candidates = [v for v in localized_texts if v.n_o_l <= n_o_l]
    if candidates:
        candidates.sort(key=lambda obj: obj.n_o_l or -1)
    return candidates


class LocalizationStorage():
    def __init__(self, localizedTexts=None):
        self._localizedTexts = defaultdict(list) # key = handle, value = list of pmtypes.LocalizedText objects
        if localizedTexts:
            self.add(*localizedTexts)

    def add(self, *localizedTexts):
        for t in localizedTexts:
            self._localizedTexts[t.Ref].append(t)

    def filterLocalizedTexts(self, requestedHandles, requestedVersion, requestedLangs, textWidths, numberOfLines):
        '''

        :param requestedHandles: list of handles
        :param requestedVersion: an integer or None
        :param requestedLangs: list of language strings
        :param textWidths: a list of integers, 0...n
        :param numberOfLines: a list of integers, 0...n
        :return: a list of LocalizedText instances
        '''
        # make integers for textWidths and numberOfLines
        if textWidths is None:
            textWidths = []
        if numberOfLines is None:
            numberOfLines = []
        if requestedHandles is None:
            requestedHandles = []
        i_nls = [int(l) for l in numberOfLines]

        if len(requestedHandles) == 0:
            # If there is no Ref ELEMENT given in the request MESSAGE, then all texts are returned in
            # msg:GetLocalizedTextResponse/msg:Text
            handles = list(self._localizedTexts.keys())
        else:
            # If there is at least one Ref ELEMENT given, then msg:GetLocalizedTextResponse/msg:Text contains all texts
            # that match the Ref ELEMENTs of the msg:GetLocalizedText request MESSAGE.
            handles = requestedHandles

        # create a flat list of all localized texts with the requested handles
        texts = []
        for h in handles:
            try:
                texts.extend(self._localizedTexts[h])
            except KeyError:
                pass

        # filter languages:
        if requestedLangs is not None and len(requestedLangs) > 0:
            texts = [t for t in texts if t.Lang in requestedLangs]

        # filter requested versions. We need to do it per language, therefore create a lookup with (ref,lang) as key
        tmp_dict = defaultdict(list)
        for t in texts:
            tmp_dict[(t.Ref, t.Lang)].append(t)
        texts = []

        if requestedVersion is None:
            # determine the highest available Version in the storage
            all = []
            for v in self._localizedTexts.values():
                all.extend(v)
            all = [a.Version for a in all if a.Version is not None]
            requestedVersion = max(all)
        # If the referenced text is not available in the specific version, then
        # msg:GetLocalizedTextResponse/msg:Text is empty
        for key, value_list in tmp_dict.items():
            texts.extend([v for v in value_list if v.Version == requestedVersion])

        # - If there is no NumberOfLines ELEMENT given in the request MESSAGE, then all texts independent of the number
        #   of lines are returned in msg:GetLocalizedTextResponse/msg:Text.
        # - If there is at least one NumberOfLines ELEMENT given, msg:GetLocalizedTextResponse/msg:Text contains texts
        #   that match the number of lines defined by the NumberOfLines ELEMENTs of the msg:GetLocalizedText request
        #   MESSAGE. Matching in this case means that the number of lines in the text is less or equal to the
        #   NumberOfLines ELEMENTs.

        if len(textWidths) > 0 or len(numberOfLines) > 0:
            if len(numberOfLines) > 0:
                # calculate number of lines for every localized text and add is as member to the object
                for t in texts:
                    t.n_o_l = _calcNoL(t.text)

            # create again dictionary by ref and language:
            tmp_dict = defaultdict(list)
            for t in texts:
                tmp_dict[(t.Ref, t.Lang)].append(t)
            tmp = []

            if len(textWidths) > 0 and len(numberOfLines) > 0:
                # now find for each combination of (width, lines) list the best match
                for key, value_list in tmp_dict.items():
                    candidates = []
                    for w in textWidths:
                        candidates1 = _textWidth_filter(value_list, w) # returns sorted list of smaller elements
                        for l in i_nls:
                            candidates2 = _n_o_l_filter(candidates1, l)
                            if len(candidates2) > 0:
                                candidates2.sort(key=lambda obj: obj.TextWidth*obj.n_o_l) # sort by area size
                                tmp.append(candidates2[-1])  # use largest one
            elif len(textWidths) > 0:
                # filter only text widths
                for key, value_list in tmp_dict.items():
                    for w in textWidths:
                        candidates = _textWidth_filter(value_list, w)  # returns sorted list of smaller elements
                        if candidates:
                            tmp.append(candidates[-1])  # use largest one

            elif len(numberOfLines) > 0:
                # filter only number of lines
                for key, value_list in tmp_dict.items():
                    for l in i_nls:
                        candidates = _n_o_l_filter(value_list, l)
                        if candidates:
                            tmp.append(candidates[-1])  # use largest one
            texts = list(tmp)
        return texts


    def getSupportedlanguages(self):
        texts = self._flatList()
        result = set()
        for text in texts:
            result.add(str(text.Lang))
        return list(result)

    def _flatList(self, refList=None):
        if refList is None:
            # If there is no Ref ELEMENT given in the request MESSAGE, then all texts are returned in
            # msg:GetLocalizedTextResponse/msg:Text
            handles = list(self._localizedTexts.keys())
        else:
            # If there is at least one Ref ELEMENT given, then msg:GetLocalizedTextResponse/msg:Text contains all texts
            # that match the Ref ELEMENTs of the msg:GetLocalizedText request MESSAGE.
            handles = refList

        # create a flat list of all localized texts with the requested handles
        texts = []
        for h in handles:
            try:
                texts.extend(self._localizedTexts[h])
            except KeyError:
                pass
        return texts


class LocalizationService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetLocalizedText', ('{}:GetLocalizedText'.format(_msg),)),
                               WSDLMessageDescription('GetLocalizedTextResponse', ('{}:GetLocalizedTextResponse'.format(_msg),)),
                               WSDLMessageDescription('GetSupportedLanguages', ('{}:GetSupportedLanguages'.format(_msg),)),
                               WSDLMessageDescription('GetSupportedLanguagesResponse', ('{}:GetSupportedLanguagesResponse'.format(_msg),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetLocalizedText', 'literal', 'literal'),
                             WSDLOperationBinding('GetSupportedLanguages', 'literal', 'literal'),)

    def __init__(self, port_type_string, sdcDevice):
        super(LocalizationService, self).__init__(port_type_string, sdcDevice )
        self.register_soapActionCallback(self._mdib.sdc_definitions.Actions.GetLocalizedText, self._onGetLocalizedText)
        self.register_soapActionCallback(self._mdib.sdc_definitions.Actions.GetSupportedLanguages, self._onGetSupportedLanguages)
        self.localizationStorage = LocalizationStorage()

    def _onGetLocalizedText(self, httpHeader, request):  # pylint:disable=unused-argument
        self._logger.debug('_onGetLocalizedText')
        requestedHandles = request.bodyNode.xpath('*/msg:Ref/text()', namespaces=nsmap) # handle strings 0...n
        requestedVersions = request.bodyNode.xpath('*/msg:Version/text()', namespaces=nsmap) # unsigned long int 0..1
        requestedLangs = request.bodyNode.xpath('*/msg:Lang/text()', namespaces=nsmap) # unsigned long int 0..n
        textWidths = request.bodyNode.xpath('*/msg:TextWidth/text()', namespaces=nsmap) # strings 0..n
        numberOfLines = request.bodyNode.xpath('*/msg:NumberOfLines/text()', namespaces=nsmap) # int 0..n
        # make integers for textWidths and numberOfLines
        i_tws = [_tw2i(w) for w in textWidths]
        i_nls = [int(l) for l in numberOfLines]

        if len(requestedVersions) > 0:
            # If the referenced text is not available in the specific version, then
            # msg:GetLocalizedTextResponse/msg:Text is empty
            requestedVersion = int(requestedVersions[0])
        else:
            requestedVersion = None

        texts = self.localizationStorage.filterLocalizedTexts(requestedHandles,
                                                              requestedVersion,
                                                              requestedLangs,
                                                              i_tws, i_nls)

        # create the response
        nsmapper = self._mdib.nsmapper
        responseSoapEnvelope = pysoap.soapenvelope.Soap12Envelope(
            nsmapper.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG))
        replyAddress = request.address.mkReplyAddress(action=self._getActionString('GetLocalizedTextResponse'))
        responseSoapEnvelope.addHeaderObject(replyAddress)
        getLocalizedTextResponseNode = etree_.Element(msgTag('GetLocalizedTextResponse'))
        self._mdib.mdib_version_group.update_node(getLocalizedTextResponseNode)

        for text in texts:
            getLocalizedTextResponseNode.append(text.asEtreeNode(msgTag('Text'), nsmap=None))
        responseSoapEnvelope.addBodyElement((getLocalizedTextResponseNode))
        return responseSoapEnvelope


    def _onGetSupportedLanguages(self, httpHeader, request):  # pylint:disable=unused-argument
        self._logger.debug('_onGetSupportedLanguages')
        languages = self.localizationStorage.getSupportedlanguages()

        nsmapper = self._mdib.nsmapper
        responseSoapEnvelope = pysoap.soapenvelope.Soap12Envelope(
            nsmapper.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG))
        replyAddress = request.address.mkReplyAddress(action=self._getActionString('GetSupportedLanguagesResponse'))
        responseSoapEnvelope.addHeaderObject(replyAddress)
        getSupportedLanguagesResponseNode = etree_.Element(msgTag('GetSupportedLanguagesResponse'))
        self._mdib.mdib_version_group.update_node(getSupportedLanguagesResponseNode)

        for lang in languages:
            n = etree_.SubElement(getSupportedLanguagesResponseNode, msgTag('Lang'))
            n.text = lang
        responseSoapEnvelope.addBodyElement((getSupportedLanguagesResponseNode))
        return responseSoapEnvelope


    def addWsdlPortType(self, parentNode):
        '''
        add wsdl:portType node to parentNode.
        xml looks like this:
        <wsdl:portType name="GetService" dpws:DiscoveryType="dt:ServiceProvider">
          <wsdl:operation name="GetMdState">
            <wsdl:input message="msg:GetLocalizedText"/>
            <wsdl:output message="msg:GetLocalizedTextResponse"/>
          </wsdl:operation>
          <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
          ...
        </wsdl:portType>
        :param parentNode:
        :return:
        '''
        if 'dt' in parentNode.nsmap:
            portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                         attrib={'name': self.port_type_string,
                                                 dpwsTag('DiscoveryType'):'dt:ServiceProvider'})
        else:
            portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns, 'portType'),
                                         attrib={'name': self.port_type_string})
        _mkWsdlTwowayOperation(portType, operationName='GetLocalizedText')
        _mkWsdlTwowayOperation(portType, operationName='GetSupportedLanguages')
