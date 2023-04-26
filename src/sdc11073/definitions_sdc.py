import os

from .definitions_base import BaseDefinitions, AbstractDataModel
from .mdib.descriptorcontainers import get_container_class as get_descriptor_container_class
from .mdib.statecontainers import get_container_class as get_state_container_class
from .namespaces import default_ns_helper as ns_hlp
from .xml_types import pm_types, msg_types, msg_qnames, pm_qnames, actions

schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')

# the following namespace definitions reflect the initial SDC standard.
# There might be changes or additions in the future, who knows...

_DPWS_SDCNamespace = ns_hlp.SDC.namespace  # pylint: disable=invalid-name

class V1Model(AbstractDataModel):

    def __init__(self):
        super().__init__()
        self._ns_hlp = ns_hlp

    def get_descriptor_container_class(self, type_qname):
        return get_descriptor_container_class(type_qname)

    def get_state_container_class(self, type_qname):
        return get_state_container_class(type_qname)

    @property
    def pm_types(self):
        return pm_types

    @property
    def pm_names(self):
        return pm_qnames

    @property
    def msg_types(self):
        return msg_types

    @property
    def msg_names(self):
        return msg_qnames

    @property
    def ns_helper(self):
        """Gives access to a module with all name spaces used"""
        return self._ns_hlp


class SDC_v1_Definitions(BaseDefinitions):  # pylint: disable=invalid-name
    DpwsDeviceType = ns_hlp.DPWS.tag('Device')
    MedicalDeviceType = ns_hlp.MDPWS.tag('MedicalDevice')
    ActionsNamespace = _DPWS_SDCNamespace
    PortTypeNamespace = _DPWS_SDCNamespace
    MedicalDeviceTypesFilter = [DpwsDeviceType, MedicalDeviceType]
    Actions = actions.Actions
    data_model = V1Model()
