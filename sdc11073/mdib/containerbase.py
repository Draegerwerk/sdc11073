import copy
import inspect
from lxml import etree as etree_
from .. import observableproperties as properties
from ..namespaces import QN_TYPE
from ..namespaces import Prefix_Namespace as Prefix

class ContainerBase(object):
    NODETYPE = None   # overwrite in derived classes! determines the value of xsi:Type attribute, must be a etree_.QName object
    NODENAME = None
    node = properties.ObservableProperty()

    # every class with containerproperties must provice a list of property names.
    # this list is needed to create sub elements in a certain order.
    # rule is : elements are sorted from this root class to derived class. Last derived class comes last.
    # Initialization is from left to right
    # This is according to the inheritance in BICEPS xml schema
    _props = tuple()  # empty tuple, this base class has no properties

    def __init__(self, nsmapper, node=None):
        self.nsmapper = nsmapper
        self.node = node
        if node is None:
            # initialize all ContainerProperties
            for dummy_name, cprop in self._sortedContainerProperties():
                cprop.initInstanceData(self)
        else:
            self._updateFromNode(node)


    def getActualValue(self, attr_name):
        ''' ignores default value and implied value, e.g. returns None if value is not present in xml'''
        return getattr(self.__class__, attr_name).getActualValue(self)


    def mkNode(self, tag=None, setXsiType=False):
        '''
        create a etree node from instance data
        :param tag: tag of the newly created node, defaults to self.NODENAME
        :return: etree node
        '''
        myTag = tag or self.NODENAME
        node = etree_.Element(myTag, nsmap=self.nsmapper.partialMap(Prefix.PM, Prefix.MSG, Prefix.XSI))
        self._updateNode(node, setXsiType)
        return node


    def _updateNode(self, node, setXsiType=False):
        '''
        create a etree node from instance data
        :param tag: tag of the newly created node, defaults to self.NODENAME
        :return: etree node
        '''
        if setXsiType and self.NODETYPE is not None:
            node.set(QN_TYPE, self.nsmapper.docNameFromQName(self.NODETYPE))
        for dummy_name, prop in self._sortedContainerProperties():
            prop.updateXMLValue(self, node)
        return node


    def _updateFromNode(self, node):
        ''' update members.
        '''
        # update all ContainerProperties
        for dummy_name, cprop in self._sortedContainerProperties():
            cprop.updateFromNode(self, node)


    def mkCopy(self, copy_node=True):
        copied = copy.copy(self)
        if copy_node:
            cpNode = copy.deepcopy(self.node)
            copied.node = cpNode
        return copied


    def _sortedContainerProperties(self):
        '''
        @return: a list of (name, object) tuples of all GenericProperties ( and subclasses)
        '''
        ret = []
        all_classes = inspect.getmro(self.__class__)
        for cls in reversed(all_classes):
            try:
                names = cls._props  # pylint:disable=protected-access
            except:
                continue
            for name in names:
                obj = getattr(cls, name)
                if obj is not None:
                    ret.append((name, obj))
        return ret


    def diff(self, other, ignore_property_names=None):
        """ compares all properties (except to be ignored ones).
        returns a list of strings that describe differences"""
        ret = []
        ignore_list = ignore_property_names or []
        my_properties = self._sortedContainerProperties()
        for name, dummy in my_properties:
            if name in ignore_list:
                continue
            my_value = getattr(self, name)
            try:
                other_value = getattr(other, name)
            except AttributeError:
                ret.append('{}={}, other does not have this attribute'.format(name, my_value))
            else:
                if isinstance(my_value, float) or isinstance(other_value, float):
                    # cast both to float, if one is a Decimal Exception might be thrown
                    if abs((float(my_value)-float(other_value))/float(my_value)) > 1e-10: # 1e-10 is good enough
                        ret.append('{}={}, other={}'.format(name, my_value, other_value))
                elif my_value != other_value:
                    ret.append('{}={}, other={}'.format(name, my_value, other_value))
        # check also if other has a different list of properties
        my_property_names = set([p[0] for p in my_properties])
        other_property_names = set([p[0] for p in other._sortedContainerProperties()])
        surplus_names = other_property_names - my_property_names
        if surplus_names:
            ret.append(f'other has more data elements:{surplus_names}')
        return ret

    def is_equal(self, other):
        return len(self.diff(other)) == 0

    def __repr__(self):
        return '{} name="{}" type={}'.format(self.__class__.__name__, self.NODENAME, self.NODETYPE)

