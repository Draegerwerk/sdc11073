from collections import OrderedDict
import urllib

class UrlSchemeError(Exception):
    pass

class SdcLocation(object):
    scheme = 'sdc.ctxt.loc'
    locationDetailRoot = 'sdc.ctxt.loc.detail'
    _url_member_mapping = (('fac', 'fac'), ('bldng', 'bld'), ('flr', 'flr'), ('poc', 'poc'), ('rm', 'rm'), ('bed', 'bed')) # urlName, attrName
    def __init__(self, fac=None, poc=None, bed=None, bld=None,  flr=None, rm=None, root=locationDetailRoot):
        self.root = root
        self.fac = fac   # facility
        self.bld = bld   # building
        self.poc = poc   # point of Care
        self.flr = flr   # floor
        self.rm = rm     # room
        self.bed = bed   # Bed
    

    def mkExtensionStringSdc(self):
        elements = self._getExtensionElementsSdc()
        values = [e[1] for e in elements]
        return '/'.join(values)


    def _getExtensionElementsSdc(self):
        '''
        :return: a list of (urlName, value) tuples
        '''
        identifiers = []
        for urlName, attrName in self._url_member_mapping:
            value = getattr(self, attrName)
            if value is None:
                value = ''
            identifiers.append((urlName, value))
        return identifiers


    @property
    def scopeStringSdc(self):
        return self._mkScopeString(self._getExtensionElementsSdc())


    def _mkScopeString(self, elements):
        identifiers = []
        queryDict= OrderedDict() # technically an OrderedDict is not necessary here, but I like to keep the Query arguments sorted. Easier testing, simple string compare ;)
        for urlName, value in elements:
            identifiers.append(value)
            if value:
                queryDict[urlName] = value
        identifiers = [urllib.parse.quote(ident) for ident in identifiers]
        slash = urllib.parse.quote('/', safe='')
        loc = slash.join(identifiers)  # this is a bit ugly, but urllib.quote does not touch slashes;
        query = urllib.parse.urlencode(queryDict)
        path = '/{}/{}'.format(urllib.parse.quote(self.root), loc)
        scopeString = urllib.parse.urlunparse(urllib.parse.ParseResult(scheme=self.scheme, netloc=None, path=path, params=None, query=query, fragment=None))
        return scopeString


    def matchingServices(self, services):
        return [s for s in services if self.serviceMatches(s)]
        

    def serviceMatches(self, service):
        return self.anyScopeStringMatches(service.getScopes())


    def anyScopeStringMatches(self, scopes):
        for s in scopes:
            if self.scopeStringMatches(s):
                return True
        return False
    
    
    def scopeStringMatches(self, scope):
        '''
        Check if location in scope is inside own location.
        :param scope: url string
        :return: boolean
        '''
        try:
            other =  self.__class__.fromScopeString(str(scope))
            return other in self
        except UrlSchemeError:
            # Scope has different scheme, no match
            return False


    def __contains__(self, other):
        if self.root != other.root:
            return False
        for my, notmy in ((self.fac, other.fac),
                          (self.bld, other.bld),
                          (self.poc, other.poc),
                          (self.flr, other.flr),
                          (self.rm, other.rm),
                          (self.bed, other.bed)):
            if my is not None:
                if my != notmy:
                    return False
        return True


    @classmethod
    def fromScopeString(cls, s):
        '''
        Construct a Location from a scope string. If url scheme is not 'sdc.ctxt.loc', an UrlSchemeError is raised
        :param s: an url
        :return: a SdcLocation object
        '''
        src = urllib.parse.urlsplit(s)
        
        if src.scheme.lower() != cls.scheme:
            raise UrlSchemeError('scheme "{}" not excepted, must be "{}"'.format(src.scheme, cls.scheme))
        dummy, root, path = src.path.split('/')
        root = urllib.parse.unquote(root)
        path = urllib.parse.unquote(path)
        queryDict = dict(urllib.parse.parse_qsl(src.query))
        # make a new argumentsDict with well known keys. This allows to ignore unknown keys that might be present in querydict
        argumentsDict = {}
        for urlName, attrName in cls._url_member_mapping:
            argumentsDict[attrName] = queryDict.get(urlName)
        
        argumentsDict['root'] = root
        return cls(**argumentsDict)
    
    
    def __eq__(self, other):
        attrNames = [e[1] for e in self._url_member_mapping]
        attrNames.append('root')
        try:
            for attrName in attrNames:
                if getattr(self, attrName) !=  getattr(other, attrName):
                    return False
            return True
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other    
    
    def __str__(self):
        return '{} {}'.format(self.__class__.__name__, self.scopeStringSdc)
        