from urllib.parse import quote, unquote, urlencode, parse_qsl, urlunparse, urlsplit, ParseResult
from collections import OrderedDict


class UrlSchemeError(Exception):
    pass


class SdcLocation:
    scheme = 'sdc.ctxt.loc'
    location_detail_root = 'sdc.ctxt.loc.detail'
    _url_member_mapping = (
        ('fac', 'fac'), ('bldng', 'bld'), ('flr', 'flr'), ('poc', 'poc'), ('rm', 'rm'),
        ('bed', 'bed'))  # urlName, attrName

    def __init__(self, fac=None, poc=None, bed=None, bld=None, flr=None, rm=None, root=location_detail_root):
        # pylint: disable=invalid-name
        self.root = root
        self.fac = fac  # facility
        self.bld = bld  # building
        self.poc = poc  # point of Care
        self.flr = flr  # floor
        self.rm = rm  # room
        self.bed = bed  # Bed

    def mk_extension_string(self):
        elements = self._get_extension_elements()
        values = [e[1] for e in elements]
        return '/'.join(values)

    def _get_extension_elements(self):
        """
        :return: a list of (urlName, value) tuples
        """
        identifiers = []
        for url_name, attr_name in self._url_member_mapping:
            value = getattr(self, attr_name)
            if value is None:
                value = ''
            identifiers.append((url_name, value))
        return identifiers

    @property
    def scope_string(self):
        return self._mk_scope_string(self._get_extension_elements())

    def _mk_scope_string(self, elements):
        identifiers = []
        query_dict = OrderedDict()  # technically an OrderedDict is not necessary here, but I like to keep the Query arguments sorted. Easier testing, simple string compare ;)
        for url_name, value in elements:
            identifiers.append(value)
            if value:
                query_dict[url_name] = value
        identifiers = [quote(ident) for ident in identifiers]
        slash = quote('/', safe='')
        loc = slash.join(identifiers)  # this is a bit ugly, but urllib.quote does not touch slashes;
        query = urlencode(query_dict)
        path = f'/{quote(self.root)}/{loc}'
        scope_string = urlunparse(
            ParseResult(scheme=self.scheme, netloc=None, path=path, params=None, query=query, fragment=None))
        return scope_string

    def matching_services(self, services):
        return [s for s in services if self._service_matches(s)]

    def _service_matches(self, service):
        return self._any_scope_string_matches(service.scopes)

    def _any_scope_string_matches(self, scopes):
        for scope in scopes:
            if self._scope_string_matches(scope):
                return True
        return False

    def _scope_string_matches(self, scope):
        """
        Check if location in scope is inside own location.
        :param scope: url string
        :return: boolean
        """
        try:
            other = self.__class__.from_scope_string(str(scope))
            return other in self
        except UrlSchemeError:
            # Scope has different scheme, no match
            return False

    def __contains__(self, other):
        if self.root != other.root:
            return False
        for my_attr, other_attr in ((self.fac, other.fac),
                                    (self.bld, other.bld),
                                    (self.poc, other.poc),
                                    (self.flr, other.flr),
                                    (self.rm, other.rm),
                                    (self.bed, other.bed)):
            if my_attr is not None:
                if my_attr != other_attr:
                    return False
        return True

    @classmethod
    def from_scope_string(cls, scope_string):
        """
        Construct a Location from a scope string. If url scheme is not 'sdc.ctxt.loc', an UrlSchemeError is raised
        :param scope_string: an url
        :return: a SdcLocation object
        """
        src = urlsplit(scope_string)

        if src.scheme.lower() != cls.scheme:
            raise UrlSchemeError(f'scheme "{src.scheme}" not excepted, must be "{cls.scheme}"')
        dummy, root, _ = src.path.split('/')
        root = unquote(root)
        query_dict = dict(parse_qsl(src.query))
        # make a new argumentsDict with well known keys. This allows to ignore unknown keys that might be present in
        # query_dict
        arguments_dict = {}
        for url_name, attr_name in cls._url_member_mapping:
            arguments_dict[attr_name] = query_dict.get(url_name)

        arguments_dict['root'] = root
        return cls(**arguments_dict)

    def __eq__(self, other):
        attr_names = [e[1] for e in self._url_member_mapping]
        attr_names.append('root')
        try:
            for attr_name in attr_names:
                if getattr(self, attr_name) != getattr(other, attr_name):
                    return False
            return True
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return f'{self.__class__.__name__} {self.scope_string}'
