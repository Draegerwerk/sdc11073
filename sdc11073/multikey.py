from collections import defaultdict, namedtuple
from threading import RLock

'''
This module implements an in-memory table with indices for faster access to objects.
Example: You have a class 
class Person:
    def __init__(self, first_name, last_name, age):
        self.first_name = first_name
        self.last_name = last_name
        self._age = age

    def getAge(self):
        return int(self._age

you can setup an in-memory table like this:
person_lookup = MultiKeyLookup()
person_lookup.addIndex('by_firstname', multikey.IndexDefinition(lambda obj: obj.first_name))
person_lookup.addIndex('by_firstname', multikey.IndexDefinition(lambda obj: obj.last_name))
person_lookup.addIndex('by_age', multikey.IndexDefinition(lambda obj: obj.getAge()))

person_lookup.addObject(Person('Peter', 'Miller', 42)
person_lookup.addObject(Person('John', 'Myers', 50)
person_lookup.addObject(Person('Agnes', 'Miller', 42)

accessing by index:
all_millers = person_lookup.by_lastname.get('Miller')
all_42_agers = person_lookup.by_age.get(42)
'''


class IndexDefinition(dict):
    ''' An index allows to group objects by values.
    This is a dictionary that has lists ob objects as value.
    Each list contains objects that have the same key member'''

    def __init__(self, getKeyFunc, indexNoneValues=True):
        '''
        :param getKeyFunc: a callable that returns a key value from a given object
        :param indexNoneValues: if True, a None key is handled like every other value.
                                if False,a None key is not added to index.
        '''
        super(IndexDefinition, self).__init__()
        self._getKeyFunc = getKeyFunc
        self._indexNoneValues = indexNoneValues

    def getOne(self, key, allowNone=False):
        try:
            result = self[key]
            if len(result) > 1:
                raise RuntimeError('getOne: key "{}" has {} objects'.format(key, len(result)))
            return result[0]
        except KeyError:
            if allowNone:
                return
            raise RuntimeError('key "{}" not found'.format(key))

    def _mkKeys(self, obj):
        key = self._getKeyFunc(obj)
        if not self._indexNoneValues and key is None:
            return
        try:
            self[key].append(obj)
        except KeyError:
            self[key] = [obj]
        return [key]

    def _rmKey(self, key, obj):
        try:
            objList = self[key]
            objList.remove(obj)
            if len(objList) == 0:
                del self[key]
        except (KeyError, ValueError):
            pass


class UIndexDefinition(IndexDefinition):
    ''' A unique Index, there can only be one object with that key'''

    def _mkKeys(self, obj):
        keys = self._getKeyFunc(obj)
        if not self._indexNoneValues and keys is None:
            return
        if isinstance(keys, list):
            raise ValueError('list of keys not allowed in UIndex: obj= {}, keys={}'.format(obj, keys))
            pass
        else:
            keys = [keys]
        for k in keys:
            if k in self:
                raise KeyError('key "{}" in already in this UIndex'.format(k))
            self[k] = [obj]
        return keys


class IndexDefinition1n(IndexDefinition):
    ''' For member values that are a list of keys (1:n relationship)'''

    def _mkKeys(self, obj):
        keys = self._getKeyFunc(obj)
        if not self._indexNoneValues and keys is None:
            return
        for k in keys:
            try:
                self[k].append(obj)
            except KeyError:
                self[k] = [obj]
        return keys


class ObjectSelector(object):
    def __init__(self, selectedObjects):
        self.objects = selectedObjects

    def find(self, **kwargs):
        ''' OR combination of args. Values are compared for equality (==), not identity (is).'''
        result = []
        for o in self.objects:
            for name, value in kwargs.items():
                try:
                    val = getattr(o, name)
                    if callable(val):
                        val = val()
                except AttributeError:
                    pass
                else:
                    if val == value:
                        result.append(o)
                        break
        return ObjectSelector(result)


_ObjRef = namedtuple('_ObjRef', 'index_dict key')  # used internally in MultiKeyLookup to keep track of all indexes.


# when we remove an object we need it to delete all indices referencing it

class MultiKeyLookup(object):

    def __init__(self):
        self._objects = set()  # contains the objects
        self._objectIDs = defaultdict(
            list)  # key = id(object), value = list of ((_idxDefs, key) tuples that reference the object
        self._idxDefs = {}  # holds UIndexDefinition Objects
        self._lock = RLock()

    @property
    def objects(self):
        return self._objects

    @property
    def lock(self):
        return self._lock

    def getIndexDict(self, indexName):
        return self._idxDefs[indexName]  # .indices

    def __getattr__(self, name):
        return self._idxDefs[name]  # .indices

    def addIndex(self, indexName, indexDefinition):
        self._idxDefs[indexName] = indexDefinition
        # add existing objects to new lookup
        for obj in self._objects:
            keys = indexDefinition._mkKeys(obj)
            for k in keys:
                self._objectIDs[id(obj)].append(_ObjRef(indexDefinition, k))

    def addObject(self, obj):
        if obj in self._objects:
            return
        with self._lock:
            self._objects.add(obj)
            self._mkIndices(obj)

    def addObjectNoLock(self, obj):
        if obj in self._objects:
            return
        self._objects.add(obj)
        self._mkIndices(obj)

    def addObjects(self, objs):
        with self._lock:
            self.addObjectsNoLock(objs)

    def addObjectsNoLock(self, objs):
        for obj in objs:
            if obj in self._objects:
                continue
            self._objects.add(obj)
            self._mkIndices(obj)

    def _mkIndices(self, obj):
        all_keys = []  # for this object
        for indexDefinition in self._idxDefs.values():
            try:
                keys = indexDefinition._mkKeys(obj)
                for k in keys:
                    all_keys.append(_ObjRef(indexDefinition, k))
            except (TypeError, AttributeError):
                pass
        self._objectIDs[id(obj)].extend(all_keys)

    def _rmIndices(self, obj):
        obj_refs = self._objectIDs.get(id(obj), [])
        for obj_ref in obj_refs:
            obj_ref.index_dict._rmKey(obj_ref.key, obj)
        del self._objectIDs[id(obj)]

    def removeObject(self, obj):
        obj_refs = self._objectIDs.get(id(obj))
        if obj_refs is None:
            return
        with self._lock:
            self._rmIndices(obj)
            self._objects.remove(obj)

    def removeObjectNoLock(self, obj):
        obj_refs = self._objectIDs.get(id(obj))
        if obj_refs is None:
            return
        self._rmIndices(obj)
        self._objects.remove(obj)

    def removeObjects(self, objs):
        with self._lock:
            self.removeObjectsNoLock(objs)

    def removeObjectsNoLock(self, objs):
        for obj in objs:
            obj_refs = self._objectIDs.get(id(obj))
            if obj_refs is None:
                continue
            self._rmIndices(obj)
            self._objects.remove(obj)

    def updateObject(self, obj):
        if obj not in self._objects:
            raise RuntimeError('object {} not known'.format(obj))
        with self._lock:
            self._rmIndices(obj)
            self._mkIndices(obj)

    def updateObjectNoLock(self, obj):
        if obj not in self._objects:
            raise RuntimeError('object {} not known'.format(obj))
        self._rmIndices(obj)
        self._mkIndices(obj)

    def updateObjects(self, objs):
        with self._lock:
            self.updateObjectsNoLock(objs)

    def updateObjectsNoLock(self, objs):
        for obj in objs:
            if obj not in self._objects:
                raise RuntimeError('object {} not known'.format(obj))
            with self._lock:
                self._rmIndices(obj)
                self._mkIndices(obj)

    def clear(self):
        with self._lock:
            for indexDefinition in self._idxDefs.values():
                indexDefinition.clear()
            self._objectIDs.clear()
            self._objects.clear()

    def find(self, **kwargs):
        sel = ObjectSelector(self._objects)
        with self._lock:
            return sel.find(**kwargs)

    def findNoLock(self, **kwargs):
        sel = ObjectSelector(self._objects)
        return sel.find(**kwargs)

