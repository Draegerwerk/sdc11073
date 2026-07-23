"""Define the ObservableProperty descriptor and its binding helpers.

An ObservableProperty must be declared as a class attribute, similar to standard python properties.
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
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any


class WeakRef:
    """Weak reference that also supports references to bound methods.

    See http://stackoverflow.com/questions/599430/why-doesnt-the-weakref-work-on-this-bound-method
    for the reason why a plain :class:`weakref.ref` is not sufficient for bound methods.
    """

    def __init__(self, item: Callable) -> None:
        """Create a weak reference to the given callable.

        :param item: the callable (function or bound method) to reference weakly.
        """
        self.reference = None
        self.method = None
        self.instance = None
        try:
            self.method = item.__func__.__name__
            self.instance = weakref.ref(item.__self__)
        except AttributeError:
            self.reference = weakref.ref(item)

    def get_ref(self) -> Callable | None:
        """Return the referenced callable, or None if it no longer exists."""
        if self.reference is not None:
            return self.reference()
        instance = self.instance()
        if instance is None:
            return None
        return getattr(instance, self.method)

    def __eq__(self, other: object) -> bool:
        try:
            if self.reference is not None:
                return self.reference == other.reference
            # Keep the second branch inside the try so that a non-WeakRef `other`
            # also raises AttributeError and is treated as not equal.
            return self.method == other.method and self.instance == other.instance  # noqa: TRY300
        except AttributeError:
            # other is of an unknown class
            return False

    # WeakRef defines __eq__ but is mutable and never used as a dict key or set member,
    # so it stays explicitly unhashable.
    __hash__ = None


class _ObservableValue:
    """Implement the basic mechanism for an observable value."""

    def __init__(self, value: Any, fire_only_on_changed_value: bool = True) -> None:
        """Initialize the observable value.

        :param value: the initial value.
        :param fire_only_on_changed_value: if True, observers are only notified when the value changes.
        """
        self.value = value
        self._fire_only_on_changed_value = fire_only_on_changed_value
        self._observers = []

    def set_value(self, value: Any) -> None:
        """Set the value and notify all bound observers.

        :param value: the new value.
        """
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

    def bind(self, func: Callable) -> None:
        """Bind a callable using a weak reference.

        :param func: the callable to notify on value changes.
        """
        self._observers.append(WeakRef(func))

    def strongbind(self, func: Callable) -> None:
        """Bind a callable using a strong reference.

        :param func: the callable to notify on value changes.
        """
        self._observers.append(func)

    def unbind(self, func: Callable) -> None:
        """Unbind a previously bound callable.

        :param func: the callable to remove.
        """
        func_ref = WeakRef(func)
        for ref in self._observers:
            if ref in (func, func_ref):
                self._observers.remove(ref)
                break

    def unbind_all(self) -> None:
        """Unbind all callables."""
        del self._observers[:]


class ObservableProperty:
    """Descriptor that stores observable data on the parent object."""

    def __init__(self, default_value: Any = None, fire_only_on_changed_value: bool = True) -> None:
        """Initialize the descriptor.

        :param default_value: the value returned before any value has been set.
        :param fire_only_on_changed_value: if True, observers are only notified when the value changes.
        """
        self._default_value = default_value
        self._fire_only_on_changed_value = fire_only_on_changed_value

    def _get_instance_data(self, obj: object) -> _ObservableValue:
        # see if we already have a _property_instance_data dictionary injected in obj
        # otherwise inject it
        try:
            lookup = obj._property_instance_data  # noqa: SLF001
        except AttributeError:
            obj._property_instance_data = {}  # noqa: SLF001
            lookup = obj._property_instance_data  # noqa: SLF001

        # see if we already have a data instance for my property instance and class instance
        # otherwise create one
        try:
            return lookup[self]
        except KeyError:
            lookup[self] = _ObservableValue(self._default_value, self._fire_only_on_changed_value)
            return lookup[self]

    def __get__(self, obj: object | None, objtype: type | None) -> Any:
        return self if obj is None else self._get_instance_data(obj).value

    def __set__(self, obj: object | None, value: Any) -> None:
        if obj is None:
            self._default_value = value
        else:
            self._get_instance_data(obj).set_value(value)

    def __delete__(self, obj: object) -> None:
        pass

    def bind(self, obj: object, func: Callable) -> None:
        """Bind a callable to this property on the given object using a weak reference.

        :param obj: the object owning this property.
        :param func: the callable to notify on value changes.
        """
        self._get_instance_data(obj).bind(func)

    def strongbind(self, obj: object, func: Callable) -> None:
        """Bind a callable to this property on the given object using a strong reference.

        :param obj: the object owning this property.
        :param func: the callable to notify on value changes.
        """
        self._get_instance_data(obj).strongbind(func)

    def unbind(self, obj: object, func: Callable) -> None:
        """Unbind a previously bound callable from this property on the given object.

        :param obj: the object owning this property.
        :param func: the callable to remove.
        """
        self._get_instance_data(obj).unbind(func)

    def unbind_all(self, obj: object) -> None:
        """Unbind all callables from this property on the given object.

        :param obj: the object owning this property.
        """
        self._get_instance_data(obj).unbind_all()

    def __repr__(self) -> str:
        return f'ObservableProperty at 0x{id(self):X}, default value={self._default_value}'


def _find_property(obj: object, name: str) -> ObservableProperty:
    """Look in the class hierarchy for the matching member.

    :param obj: the object whose class hierarchy is searched.
    :param name: the name of the member to look for.
    :return: the matching ObservableProperty.
    """
    classes = inspect.getmro(
        obj.__class__,
    )  # getmro returns a tuple of class base classes, including class, in method resolution order

    for cls in classes:  # find the first class that has the expected member
        if name in cls.__dict__:
            return cls.__dict__[name]
    raise KeyError(name)  # if no class matches, raise KeyError


def bind(obj: object, **kwargs: Callable) -> None:
    """Bind callables with a weak reference.

    Use this bind method for all 'normal' callables like functions or methods.
    The advantage is that the garbage collector can remove objects even if they are referenced by ObservableProperty.
    ObservableProperty silently removes the callable if it no longer exists.
    This method does not work with lambda expressions!

    :param obj: an object with ObservableProperty member(s)
    :param kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable.
    """
    for name, func in kwargs.items():
        prop = _find_property(obj, name)
        prop.bind(obj, func)


def strongbind(obj: object, **kwargs: Callable) -> None:
    """Bind callables with a strong reference.

    This method also works with lambda expressions, but you must unbind the callable before the garbage
    collector can delete it.

    :param obj: an object with ObservableProperty member(s)
    :param kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable.
    """
    for name, func in kwargs.items():
        prop = _find_property(obj, name)
        prop.strongbind(obj, func)


def unbind(obj: object, **kwargs: Callable) -> None:
    """Unbind callables that were bound before.

    :param obj: an object with ObservableProperty member(s)
    :param kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable.
                    Unbinding an unknown callable is allowed, in this cases nothing changes.
    """
    for name, func in kwargs.items():
        prop = _find_property(obj, name)
        prop.unbind(obj, func)


def unbind_all(obj: object, *propertyNames: str) -> None:  # noqa: N803
    # propertyNames keeps its camelCase name because it is part of the public API.
    """Unbind all callables that were bound before.

    :param obj: an object with ObservableProperty member(s)
    :param propertyNames: list of strings, each string names an ObservableProperty.
    """
    for name in propertyNames:
        prop = _find_property(obj, name)
        prop.unbind_all(obj)


@contextmanager
def bound_context(obj: object, **kwargs: Callable) -> Generator[None]:
    """Context manager for bind / unbind sequence."""
    bind(obj, **kwargs)
    try:
        yield
    finally:
        unbind(obj, **kwargs)


@contextmanager
def strong_bound_context(obj: object, **kwargs: Callable) -> Generator[None]:
    """Context manager for strongbind / unbind sequence."""
    strongbind(obj, **kwargs)
    try:
        yield
    finally:
        unbind(obj, **kwargs)
