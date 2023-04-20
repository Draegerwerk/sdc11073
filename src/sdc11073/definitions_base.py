import os
from abc import ABC, abstractmethod

schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')


class ProtocolsRegistry(type):
    """
    base class that has the only purpose to register classes that use this as metaclass
    """
    protocols = []

    def __new__(cls, name, *arg, **kwarg):
        new_cls = super().__new__(cls, name, *arg, **kwarg)
        if name != 'BaseDefinitions':  # ignore the base class itself
            cls.protocols.append(new_cls)
        return new_cls


class AbstractDataModel(ABC):

    @abstractmethod
    def get_descriptor_container_class(self, type_qname):
        raise NotImplementedError

    def mk_descriptor_container(self, type_qname, handle, parent_descriptor):
        cls = self.get_descriptor_container_class(type_qname)
        if parent_descriptor is not None:
            ret = cls(handle, parent_descriptor.Handle)
            ret.set_source_mds(parent_descriptor.source_mds)
        else:
            ret = cls(handle, None)
        return ret

    @abstractmethod
    def get_state_container_class(self, type_qname):
        raise NotImplementedError

    def get_state_class_for_descriptor(self, descriptor_container):
        state_class_qtype = descriptor_container.STATE_QNAME
        if state_class_qtype is None:
            raise TypeError(f'No state association for {descriptor_container.__class__.__name__}')
        return self.get_state_container_class(state_class_qtype)

    def mk_state_container(self, descriptor_container):
        cls = self.get_state_class_for_descriptor(descriptor_container)
        if cls is None:
            raise TypeError(
                f'No state container class for descr={descriptor_container.__class__.__name__}, '
                f'name={descriptor_container.NODETYPE}, '
                f'type={descriptor_container.nodeType}')
        return cls(descriptor_container)

    @property
    @abstractmethod
    def pm_types(self):
        """Gives access to a module with participant model types"""
        raise NotImplementedError

    @property
    @abstractmethod
    def pm_names(self):
        """Gives access to a module with all qualified names of the BICEPS participant model"""
        raise NotImplementedError

    @property
    @abstractmethod
    def msg_types(self):
        """Gives access to a module with message model types"""
        raise NotImplementedError

    @property
    @abstractmethod
    def msg_names(self):
        """Gives access to a module with all qualified names of the BICEPS message model"""
        raise NotImplementedError

    @property
    @abstractmethod
    def ns_helper(self):
        """Gives access to a module with all name spaces used"""
        raise NotImplementedError


# definitions that group all relevant dependencies for BICEPS versions
class BaseDefinitions(metaclass=ProtocolsRegistry):
    """ Base class for central definitions used by SDC.
    It defines namespaces and handlers for the protocol.
    Derive from this class in order to define different protocol handling."""
    # set the following values in derived classes:
    MedicalDeviceType = None  # a QName, needed for types_match method
    ActionsNamespace = None  # needed for wsdl generation
    PortTypeNamespace = None  # needed for wsdl generation
    MedicalDeviceTypesFilter = None  # list of QNames that are used / expected in "types" of wsdiscovery
    Actions = None
    data_model = None  # AbstractDataModel instance

    @classmethod
    def types_match(cls, types):
        """ This method checks if this definition can be used for the provided types."""
        return cls.MedicalDeviceType in types
