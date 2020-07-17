import base64
import hashlib
from lxml import etree as etree_
from sdc11073.namespaces import Prefix_Namespace as Prefix
from sdc11073.namespaces import mdpwsTag


def B64Sha1(valueString):
    """Gets standard base64 coded sha1 sum of input file."""
    # pylint: disable=E1101
    if isinstance(valueString, str):
        valueString = valueString.encode('utf-8')
    return base64.standard_b64encode(hashlib.sha1(valueString).digest()).decode()

def Sha1(valueString):
    """Gets standard base64 coded sha1 sum of input file."""
    # pylint: disable=E1101
    if isinstance(valueString, str):
        valueString = valueString.encode('utf-8')
    return hashlib.sha1(valueString).hexdigest()


class SafetyInfoHeader(object):
    def __init__(self, dualChannelValues, safetyContextValues, algorithm=None):
        self.dualChannelValues = dualChannelValues
        self.safetyContextValues = safetyContextValues
        self._algorithm = algorithm


    def _asEtreeNode(self):
        safetyInfo = etree_.Element(mdpwsTag('SafetyInfo'), nsmap=Prefix.partialMap(Prefix.MDPWS))
        if self.dualChannelValues:
            dualChannel = etree_.SubElement(safetyInfo, mdpwsTag('DualChannel'))
            for ref, value in self.dualChannelValues.items():
                dcValue = etree_.SubElement(dualChannel, mdpwsTag('DcValue'))
                dcValue.set('ReferencedSelector', ref)
                algorithm = self._algorithm or Sha1
                dcValue.text = algorithm(value)
        
        if self.safetyContextValues:
            safetyContext = etree_.SubElement(safetyInfo, mdpwsTag('SafetyContext'))
            for ref, value in self.safetyContextValues.items():
                ctxtValue = etree_.SubElement(safetyContext, mdpwsTag('CtxtValue'))
                ctxtValue.set('ReferencedSelector', ref)
                ctxtValue.text = value
        return safetyInfo


    def asEtreeSubNode(self, rootNode):
        rootNode.append(self._asEtreeNode())

    @classmethod
    def fromEtreeNode(cls, rootNode): #pylint: disable=unused-argument
        raise NotImplementedError



class _Selector(object):
    def __init__(self, xpathString):
        self.xpathString = xpathString



class DualChannelDef(object):
    ''' Definition is located in MdDescription'''
    def __init__(self, algorithm, transform, selectorDict):
        self.algorithm = algorithm
        self.transform = transform
        self.selectorDict = selectorDict
    
    
    @classmethod
    def fromEtreeNode(cls, node):
        algorithm = node.get('Algorithm')
        transform = node.get('Transform')
        selectorDict = {}
        for s in node.findall(mdpwsTag('Selector')):
            id_ = s.get('Id')
            text = s.text
            selectorDict[id_] = _Selector(text)
        
        return cls(algorithm, transform, selectorDict)
        