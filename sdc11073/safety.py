import base64
import hashlib

from lxml import etree as etree_

#raise  ImportError

def base64_sha1(value_string):
    """Gets standard base64 coded sha1 sum of input file."""
    # pylint: disable=E1101
    if isinstance(value_string, str):
        value_string = value_string.encode('utf-8')
    return base64.standard_b64encode(hashlib.sha1(value_string).digest()).decode()


def sha1(value_string):
    """Gets standard base64 coded sha1 sum of input file."""
    # pylint: disable=E1101
    if isinstance(value_string, str):
        value_string = value_string.encode('utf-8')
    return hashlib.sha1(value_string).hexdigest()


class SafetyInfoHeader:
    def __init__(self, ns_helper, dual_channel_values, safety_context_values, algorithm=None):
        self._ns_hlp = ns_helper
        self.dual_channel_values = dual_channel_values
        self.safety_context_values = safety_context_values
        self._algorithm = algorithm

    def as_etree_node(self):
        safety_info = etree_.Element(self._ns_hlp.mdpwsTag('SafetyInfo'),
                                     nsmap=self._ns_hlp.partial_map(self._ns_hlp.MDPWS))
        if self.dual_channel_values:
            dual_channel = etree_.SubElement(safety_info, self._ns_hlp.mdpwsTag('DualChannel'))
            for ref, value in self.dual_channel_values.items():
                dc_value = etree_.SubElement(dual_channel, self._ns_hlp.mdpwsTag('DcValue'))
                dc_value.set('ReferencedSelector', ref)
                algorithm = self._algorithm or sha1
                dc_value.text = algorithm(value)

        if self.safety_context_values:
            safety_context = etree_.SubElement(safety_info, self._ns_hlp.mdpwsTag('SafetyContext'))
            for ref, value in self.safety_context_values.items():
                ctxt_value = etree_.SubElement(safety_context, self._ns_hlp.mdpwsTag('CtxtValue'))
                ctxt_value.set('ReferencedSelector', ref)
                ctxt_value.text = value
        return safety_info

    @classmethod
    def from_etree_node(cls, root_node):  # pylint: disable=unused-argument
        raise NotImplementedError


class _Selector:
    def __init__(self, xpath_string):
        self.xpath_string = xpath_string


class DualChannelDef:
    ''' Definition is located in MdDescription'''

    def __init__(self, ns_helper, algorithm, transform, selector_dict):
        self._ns_hlp = ns_helper
        self.algorithm = algorithm
        self.transform = transform
        self.selector_dict = selector_dict

    @classmethod
    def from_etree_node(cls, node):
        algorithm = node.get('Algorithm')
        transform = node.get('Transform')
        selector_dict = {}
        for selector in node.findall(self._ns_hlp.mdpwsTag('Selector')):
            id_ = selector.get('Id')
            text = selector.text
            selector_dict[id_] = _Selector(text)

        return cls(algorithm, transform, selector_dict)
