''' This module defines an ObservableProperty class.
An ObservablePropery must be declared as class attribute, similar to standard python properties.
You can bind callables to an ObservableProperty. The callable is called when the property value is set.

Example:
>>> class MyBaseClass(object):
>>>     prop1 = ObservableProperty(21)
>>>     prop2 = ObservableProperty(22)
>>>     
>>>     def __init__(self):
>>>         do_something()
>>> 
>>> class Observer(object):
>>>     def onProp1Changed(value):
>>>         print 'prop1=', value
>>>     def onProp21Changed(value):
>>>         print 'prop2=', value
>>> 
>>> actor = MyBaseClass()
>>> observer = Observer()
>>> bind(actor, prop1=observer.onProp1Changed, prop2=observer.onProp2Changed)

>>> actor.prop1=42
< prop1= 42
>>> actor.prop2='Hello World'
< prop2= Hello World

'''
import weakref
import inspect
from contextlib import contextmanager


class WeakRef:
    ''' This Weaf Ref implementation allows to hold references to bound methods.
    => see http://stackoverflow.com/questions/599430/why-doesnt-the-weakref-work-on-this-bound-method'''
    def __init__ (self, item):
        self.reference = None
        self.method = None
        self.instance = None        
        try:
            self.method   = item.__func__.__name__
            self.instance = weakref.ref (item.__self__)
        except AttributeError:
            self.reference = weakref.ref (item)


    def getRef(self):
        if self.reference is not None:
            return self.reference ()
        instance = self.instance ()
        if instance == None:
            return None
        return getattr (instance, self.method)


    def __eq__(self, other):
        try:
            if self.reference is not None:
                return self.reference == other.reference
            return self.method == other.method and self.instance == other.instance
        except AttributeError:
            # other is of an unknown class 
            return False



class _ObservableValue(object):
    ''' Implements the basic mechanism for an observable value. '''
    def __init__(self, value, fireOnlyOnChangedValue=True):
        self.value = value
        self._fireOnlyOnChangedValue = fireOnlyOnChangedValue
        self._observers = []


    def setValue(self, value):
        if value == self.value and self._fireOnlyOnChangedValue:
            return
        self.value = value
        obsoleteRefs=[]
        # now call all listeners. Keep track of obsolete weak refeences
        for ref in self._observers[:]: # make a copy of list, content might change during iteration
            try:
                func = ref.getRef()
            except AttributeError: # no Weakref instance => strong reference, use ref directly
                func = ref
            if func is None:
                obsoleteRefs.append(ref)
            else:
                self._callFunc(func)
        for ref in obsoleteRefs:
            try:
                self._observers.remove(ref)
            except ValueError: # e.g. has been deleted by someone else in different thread.
                pass


    def _callFunc(self, func):
        func(self.value)


    def bind(self, func):
        self._observers.append(WeakRef(func))


    def strongbind(self, func):
        self._observers.append(func)


    def unbind(self, func):
        funcRef = WeakRef(func)
        for ref in self._observers:
            if ref == func or ref == funcRef:
                self._observers.remove(ref)
                break


    def unbindAll(self):
        del self._observers[:]



class ObservableProperty(object):
    ''' stores data in parent obj '''
    def __init__(self, defaultValue=None, fireOnlyOnChangedValue=True):
        self._defaultValue = defaultValue
        self._fireOnlyOnChangedValue = fireOnlyOnChangedValue


    def _getInstanceData(self, obj):
        # see if we already have a _PropertyInstanceData dictionary injected in obj
        # otherwise inject it
        try:
            lookup =  obj._Property2InstanceData
        except AttributeError:
            obj._Property2InstanceData = dict()
            lookup = obj._Property2InstanceData
        
        # see if we already have a data instance for my property instance and class instance
        # otherwise create one
        try:    
            return lookup[self]
        except KeyError:
            lookup[self] = _ObservableValue(self._defaultValue, self._fireOnlyOnChangedValue)
            return lookup[self]


    def __get__(self,  obj, objtype):
        return self if obj is None else self._getInstanceData(obj).value


    def __set__(self, obj, value):
        if obj is None: 
            self._defaultvalue = value
        else:
            self._getInstanceData(obj).setValue(value)


    def __delete__(self, obj):
        pass


    def bind(self, obj, func):
        self._getInstanceData(obj).bind(func)


    def strongbind(self, obj, func):
        self._getInstanceData(obj).strongbind(func)


    def unbind(self, obj, func):
        self._getInstanceData(obj).unbind(func)


    def unbindAll(self, obj):
        self._getInstanceData(obj).unbindAll()


    def __repr__(self):
        return 'ObservableProperty at 0x{1:X}, default value= {0}'.format(self._defaultValue, id(self))


def _findProperty(obj, name):
    ''' Helper that looks in class hierarchy for matching member
    '''
    classes = inspect.getmro(obj.__class__) # getmro returns a tuple of class base classes, including class, in method resolution order
    
    for cls in classes:   # find the first class that has the expected member
        try:
            return cls.__dict__[name]
        except KeyError:
            pass
    raise KeyError(name) # if no class matches, raise KeyError
    

def bind (obj, **kwargs):
    ''' bind callables with a weak reference.
    Use this bind method for all 'normal' callables like functions or methods.
    The advantage is that the garbage collector can remove objects even if they are referenced by ObservableProperty.
    ObservableProperty silently removes the callable if it no longer exists.
    This method does not work with lambda expressions! 
    @param obj: an object with ObservableProperty member(s)
    @param **kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable.'''
    for name, func in kwargs.items():
        p = _findProperty(obj, name)
        p.bind(obj, func)


def strongbind (obj, **kwargs):
    ''' bind callables with a strong reference.
     This method also works with lambda expressions, but you must unbind the callable before the garbage collector can delete it.'''
    for name, func in kwargs.items():
        p = _findProperty(obj, name)
        p.strongbind(obj, func)


def unbind (obj, **kwargs):
    ''' unbind callables that were bound before.
    @param obj: an object with ObservableProperty member(s)
    @param **kwargs: name of parameter must match the name of an ObservableProperty, value must be a callable.
                    Unbinding an unknown callable is allowed, in this cases nothing changes. '''
    for name, func in kwargs.items():
        p = _findProperty(obj, name)
        p.unbind(obj, func)


def unbindAll (obj, *propertyNames):
    ''' unbind all callables that were bound before.
    @param obj: an object with ObservableProperty member(s)
    @param *propertyNames: list of strings , each string names an ObservableProperty.
    '''
    for name in propertyNames:
        p = _findProperty(obj, name)
        p.unbindAll(obj)


@contextmanager
def boundContext(obj, **kwargs):
    ''' context manager for bind / unbind sequence.'''
    bind(obj, **kwargs)
    try:
        yield
    finally:
        unbind(obj, **kwargs)


@contextmanager
def strongboundContext(obj, **kwargs):
    ''' context manager for strongbind / unbind sequence.'''
    strongbind(obj, **kwargs)
    try:
        yield
    finally:
        unbind(obj, **kwargs)

