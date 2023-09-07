"""
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
person_lookup.add_index('by_firstname', multikey.IndexDefinition(lambda obj: obj.first_name))
person_lookup.add_index('by_firstname', multikey.IndexDefinition(lambda obj: obj.last_name))
person_lookup.add_index('by_age', multikey.IndexDefinition(lambda obj: obj.getAge()))

person_lookup.add_object(Person('Peter', 'Miller', 42)
person_lookup.add_object(Person('John', 'Myers', 50)
person_lookup.add_object(Person('Agnes', 'Miller', 42)

accessing by index:
all_millers = person_lookup.by_lastname.get('Miller')
all_42_agers = person_lookup.by_age.get(42)
"""

from collections import defaultdict, namedtuple
from threading import RLock
import warnings


class IndexDefinition(dict):
    """ An index allows to group objects by values.
    This is a dictionary that has lists ob objects as value.
    Each list contains objects that have the same key member"""

    def __init__(self, get_key_func, index_none_values=True):
        """
        :param get_key_func: a callable that returns a key value from a given object
        :param index_none_values: if True, a None key is handled like every other value.
                                if False,a None key is not added to index.
        """
        super().__init__()
        self._get_key_func = get_key_func
        self._index_none_values = index_none_values

    def get_one(self, key, allow_none=False):
        """
        returns one object instead of a list (like get method does)
        It raises a ValueError if there are multiple values available for the key.
        It raises a KeyError if allow_none is False and the key is not present.
        :param key:
        :param allow_none:
        :return:
        """
        if allow_none:
            result = self.get(key)
            if result is None:
                return result
        else:
            result = self[key]
        if len(result) > 1:
            raise ValueError(f'get_one: key "{key}" has {len(result)} objects')
        return result[0]

    def mk_keys(self, obj):
        key = self._get_key_func(obj)
        if not self._index_none_values and key is None:
            return None
        try:
            self[key].append(obj)
        except KeyError:
            self[key] = [obj]
        return [key]

    def rm_key(self, key, obj):
        try:
            obj_list = self[key]
            obj_list.remove(obj)
            if len(obj_list) == 0:
                del self[key]
        except (KeyError, ValueError):
            pass


class UIndexDefinition(IndexDefinition):
    """ A unique Index, there can only be one object with that key"""

    def mk_keys(self, obj):
        keys = self._get_key_func(obj)
        if not self._index_none_values and keys is None:
            return None
        if isinstance(keys, list):
            raise ValueError(f'list of keys not allowed in UIndex: obj={obj}, keys={keys}')
        keys = [keys]
        for k in keys:
            if k in self:
                raise KeyError(f'key "{k}" in already in this UIndex')
            self[k] = [obj]
        return keys


class IndexDefinition1n(IndexDefinition):
    """ For member values that are a list of keys (1:n relationship)"""

    def mk_keys(self, obj):
        keys = self._get_key_func(obj)
        if not self._index_none_values and keys is None:
            return None
        for k in keys:
            try:
                self[k].append(obj)
            except KeyError:
                self[k] = [obj]
        return keys


class ObjectSelector:
    def __init__(self, selected_objects):
        self.objects = selected_objects

    def find(self, **kwargs):
        """ OR combination of args. Values are compared for equality (==), not identity (is)."""
        result = []
        for obj in self.objects:
            for name, value in kwargs.items():
                try:
                    val = getattr(obj, name)
                    if callable(val):
                        val = val()
                except AttributeError:
                    pass
                else:
                    if val == value:
                        result.append(obj)
                        break
        return ObjectSelector(result)


_ObjRef = namedtuple('_ObjRef', 'index_dict key')  # used internally in MultiKeyLookup to keep track of all indexes.


# when we remove an object we need it to delete all indices referencing it

class MultiKeyLookup:

    def __init__(self):
        self._objects = set()  # contains the objects
        self._object_ids = defaultdict(
            list)  # key = id, value = list of ((_idx_defs, key) tuples that reference the object
        self._idx_defs = {}  # holds UIndexDefinition Objects
        self._lock = RLock()

    @property
    def objects(self):
        return self._objects

    @property
    def lock(self):
        return self._lock

    # def getIndexDict(self, index_name):
    #     return self._idx_defs[index_name]  # .indices

    def __getattr__(self, name):
        return self._idx_defs[name]  # .indices

    def add_index(self, index_name, index_definition):
        self._idx_defs[index_name] = index_definition
        # add existing objects to new lookup
        for obj in self._objects:
            keys = index_definition.mk_keys(obj)
            for k in keys:
                self._object_ids[id(obj)].append(_ObjRef(index_definition, k))

    def add_object(self, obj):
        if obj in self._objects:
            return
        with self._lock:
            self._objects.add(obj)
            self._mk_indices(obj)

    def add_object_no_lock(self, obj):
        if obj in self._objects:
            return
        self._objects.add(obj)
        self._mk_indices(obj)

    def add_objects(self, objects):
        with self._lock:
            self.add_objects_no_lock(objects)

    def add_objects_no_lock(self, objects):
        for obj in objects:
            if obj in self._objects:
                continue
            self._objects.add(obj)
            self._mk_indices(obj)

    def _mk_indices(self, obj):
        all_keys = []  # for this object
        for index_definition in self._idx_defs.values():
            try:
                keys = index_definition.mk_keys(obj)
                for k in keys:
                    all_keys.append(_ObjRef(index_definition, k))
            except (TypeError, AttributeError):
                pass
        self._object_ids[id(obj)].extend(all_keys)

    def _rm_indices(self, obj):
        obj_refs = self._object_ids.get(id(obj), [])
        for obj_ref in obj_refs:
            obj_ref.index_dict.rm_key(obj_ref.key, obj)
        del self._object_ids[id(obj)]

    def remove_object(self, obj):
        obj_refs = self._object_ids.get(id(obj))
        if obj_refs is None:
            return
        with self._lock:
            self._rm_indices(obj)
            self._objects.remove(obj)

    def remove_object_no_lock(self, obj):
        obj_refs = self._object_ids.get(id(obj))
        if obj_refs is None:
            return
        self._rm_indices(obj)
        self._objects.remove(obj)

    def remove_objects(self, objects):
        with self._lock:
            self.remove_objects_no_lock(objects)

    def remove_objects_no_lock(self, objects):
        for obj in objects:
            obj_refs = self._object_ids.get(id(obj))
            if obj_refs is None:
                continue
            self._rm_indices(obj)
            self._objects.remove(obj)

    def update_object(self, obj):
        if obj not in self._objects:
            raise ValueError(f'object {obj} not known')
        with self._lock:
            self._rm_indices(obj)
            self._mk_indices(obj)

    def update_object_no_lock(self, obj):
        if obj not in self._objects:
            raise ValueError(f'object {obj} not known')
        self._rm_indices(obj)
        self._mk_indices(obj)

    def update_objects(self, objs):
        with self._lock:
            self.update_objects_no_lock(objs)

    def update_objects_no_lock(self, objs):
        for obj in objs:
            if obj not in self._objects:
                raise ValueError(f'object {obj} not known')
            with self._lock:
                self._rm_indices(obj)
                self._mk_indices(obj)

    def clear(self):
        with self._lock:
            for index_definition in self._idx_defs.values():
                index_definition.clear()
            self._object_ids.clear()
            self._objects.clear()

    def find(self, **kwargs):
        sel = ObjectSelector(self._objects)
        with self._lock:
            return sel.find(**kwargs)

    def find_no_lock(self, **kwargs):
        sel = ObjectSelector(self._objects)
        return sel.find(**kwargs)
