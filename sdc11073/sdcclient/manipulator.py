

class RequestManipulator:
    '''
    This class can be used to inspect or manipulate output created by the sdc client.
    Output creation is done in three steps: first a pysdc.pysoap.soapenvelope.Soap12Envelope is created,
    then a (libxml) etree is created from its content, anf finally a bytestring is generated from the etree.
    The sdc client calls corresponding methods of the manipulator object after every step.
    If the method returns something different from None, this returned value will be used as input for the next step.
    '''
    def __init__(self, cb_soapenvelope=None, cb_xml=None, cb_string=None):
        '''

        :param cb_soapenvelope: a callback that gets the SoapEnvelope instance as parameter
        :param cb_xml:  a callback that gets the etree instance as parameter
        :param cb_string:  a callback that gets the output string as parameter
        '''
        self.cb_soapenvelope = cb_soapenvelope
        self.cb_xml = cb_xml
        self.cb_string = cb_string

    def manipulate_soapenvelope(self, soap_envelope):
        if callable(self.cb_soapenvelope):
            return self.cb_soapenvelope(soap_envelope)

    def manipulate_domtree(self, domtree):
        if callable(self.cb_xml):
            return self.cb_xml(domtree)

    def manipulate_string(self, xml_string):
        if callable(self.cb_string):
            return self.cb_string(xml_string)

