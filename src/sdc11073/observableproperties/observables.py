""" This module defines an ObservableProperty class.
An ObservablePropery must be declared as class attribute, similar to standard python properties.
You can bind callables to an ObservableProperty. The callable is called when the property value is set.

Example:
-------
>>> class MyBaseClass:
>>>     prop1 = ObservableProperty(21)
>>>     prop2 = ObservableProperty(22)
>>>
>>>     def __init__(self):
>>>         do_something()
>>>
>>> class Observer:
>>>     def onProp1Changed(self, value):
>>>         print 'prop1=', value
>>>     def onProp21Changed(self, value):
>>>         print 'prop2=', value
>>>
>>> actor = MyBaseClass()
>>> observer = Observer()
>>> bind(actor, prop1=observer.onProp1Changed, prop2=observer.onProp2Changed)

>>> actor.prop1=42
< prop1= 42
>>> actor.prop2='Hello World'
< prop2= Hello World

"""
import contextlib
import inspect
import weakref
from contextlib import contextmanager


class WeakRef:
    """ This Weak Ref implementation allows to hold references to bound methods.
    => see http://stackoverflow.com/questions/599430/why-doesnt-the-weakref-work-on-this-bound-method"""

    def __init__(self, item):
        self.reference = None
        self.method = None
        self.instance = None
        try:
            self.method = item.__func__.__name__
            self.instance = weakref.ref(item.__self__)
        except AttributeError:
            self.reference = weakref.ref(item)

    def get_ref(self):
        if self.reference is not None:
            return self.reference()
        instance = self.instance()
        if instance is None:
            return None
        return getattr(instance, self.method)

    def __eq__(self, other):
        try:
            if self.reference is not None:
                return self.reference == other.reference
            return self.method == other.method and self.instance == other.instance
        except AttributeError:
            # other is of an unknown class
            return False


class _ObservableValue:
    """ Implements the basic mechanism for an observable value. """

    def __init__(self, value, fire_only_on_changed_value=True):
        self.value = value
        self._fire_only_on_changed_value = fire_only_on_changed_value
        self._observers = []

    def set_value(self, value):
        if value == self.value and self._fire_only_on_changed_value:
            return
        self.value = value
        obsolete_refs = []
        # now call all listeners. Keep track of obsolete weak references
        for ref in self._observers[:]:  # make a copy of list, content might change during iteration
            try:
                func = ref.get_ref()
            except AttributeError:  # no Weakref instance => strong reference, use ref directly
                func = ref
            if func is None:
                obsolete_refs.append(ref)
            else:
                func(self.value)  # call func
        for ref in obsolete_refs:
            with contextlib.suppress(ValueError):  # e.g. has been deleted by someone else in different thread
                self._observers.remove(ref)

    def bind(self, func):
        self._observers.append(WeakRef(func))

    def strongbind(self, func):
        self._observers.append(func)

    def unbind(self, func):
        func_ref = WeakRef(func)
        for ref in self._observers:
            if ref in (func, func_ref):
                self._observers.remove(ref)
                break

    def unbind_all(self):
        del self._observers[:]


class ObservableProperty:
    """ stores data in parent obj """

    def __init__(self, default_value=None, fire_only_on_changed_value=True):
        self._default_value = default_value
        self._fire_only_on_changed_value = fire_only_on_changed_value

    def _get_instance_data(self, obj):
        # see if we already have a _property_instance_data dictionary injected in obj
        # otherwise inject it
        # pylint: disable=protected-access
        try:
            lookup = obj._property_instance_data
        except AttributeError:
            obj._property_instance_data = {}
            lookup = obj._property_instance_data
        # pylint: enable=protected-access

        # see if we already have a data instance for my property instance and class instance
        # otherwise create one
        try:
            return lookup[self]
        except KeyError:
            lookup[self] = _ObservableValue(self._default_value, self._fire_only_on_changed_value)
            return lookup[self]

    def __get__(self, obj, objtype):
        return self if obj is None else self._get_instance_data(obj).value

    def __set__(self, obj, value):
        if obj is None:
            self._default_value = value
        else:
            self._get_instance_data(obj).set_value(value)

    def __delete__(self, obj):
        pass

    def bind(self, obj, func):
        self._get_instance_data(obj).bind(func)

    def strongbind(self, obj, func):
        self._get_instance_data(obj).strongbind(func)

    def unbind(self, obj, func):
        self._get_instance_data(obj).unbind(func)

    def unbind_all(self, obj):
        self._get_instance_data(obj).unbind_all()

    def __repr__(self):
        return f'ObservableProperty at 0x{id(self):X}, default value={self._default_value}'


def _find_property(obj, name):
    """ Helper that looks in class hierarchy for matching member
    """
    classes = inspect.getmro(
        obj.__class__)  # getmro returns a tuple of class base classes, including class, in method resolution order

    for cls in classes:  # find the first class that has the expected member
        try:
            return cls.__dict__[name]
        except KeyError:
            pass
    raise KeyError(name)  # if no class matches, raise KeyError


def bind(obj, **kwargs):
    """ bind callables with a weak reference.
    Use this bind method for all 'normal' callables like functions or methods.
    The advantage is that the garbage collector can remove objects even if they are referenced by ObservableProperty.
    ObservableProperty silently removes the callable if it no longer exists.
    This method does not work with lambda expressions!
    :param obj: an object with ObservableProperty member(s)
    :param kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable."""
    for name, func in kwargs.items():
        prop = _find_property(obj, name)
        prop.bind(obj, func)


def strongbind(obj, **kwargs):
    """ bind callables with a strong reference.
     This method also works with lambda expressions, but you must unbind the callable before the garbage collector can delete it."""
    for name, func in kwargs.items():
        prop = _find_property(obj, name)
        prop.strongbind(obj, func)


def unbind(obj, **kwargs):
    """ unbind callables that were bound before.
    :param obj: an object with ObservableProperty member(s)
    :param kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable.
                    Unbinding an unknown callable is allowed, in this cases nothing changes. """
    for name, func in kwargs.items():
        prop = _find_property(obj, name)
        prop.unbind(obj, func)


def unbind_all(obj, *propertyNames):
    """ unbind all callables that were bound before.
    :param obj: an object with ObservableProperty member(s)
    :param propertyNames: list of strings, each string names an ObservableProperty.
    """
    for name in propertyNames:
        prop = _find_property(obj, name)
        prop.unbind_all(obj)


@contextmanager
def bound_context(obj, **kwargs):
    """ context manager for bind / unbind sequence."""
    bind(obj, **kwargs)
    try:
        yield
    finally:
        unbind(obj, **kwargs)


@contextmanager
def strong_bound_context(obj, **kwargs):
    """ context manager for strongbind / unbind sequence."""
    strongbind(obj, **kwargs)
    try:
        yield
    finally:
        unbind(obj, **kwargs)
