from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qsl, quote, unquote, urlencode, urlsplit, urlunparse

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073.wsdiscovery.service import Service

    loc_param_value_tuple = tuple[str, str]  # e.g. ('bed', 'Bed 42') => means Bed 42 is value of bed.


class UrlSchemeError(Exception):
    """Indicate that the scheme is wrong."""


class SdcLocation:
    """SdcLocation contains all parameters that define a location."""

    scheme = 'sdc.ctxt.loc'
    location_detail_root = 'sdc.ctxt.loc.detail'
    _url_member_mapping = (
        ('fac', 'fac'), ('bldng', 'bld'), ('flr', 'flr'), ('poc', 'poc'), ('rm', 'rm'),
        ('bed', 'bed'))  # urlName, attrName

    def __init__(self,  # noqa: PLR0913
                 fac: str | None = None,
                 poc: str | None = None,
                 bed: str | None = None,
                 bld: str | None = None,
                 flr: str | None = None,
                 rm: str | None = None,
                 root: str = location_detail_root):
        # pylint: disable=invalid-name
        self.root = root
        self.fac = fac  # facility
        self.bld = bld  # building
        self.poc = poc  # point of Care
        self.flr = flr  # floor
        self.rm = rm  # room
        self.bed = bed  # Bed

    def mk_extension_string(self) -> str:
        """Create a string usable in LocationContextStateContainer."""
        elements = self._get_extension_elements()
        values = [e[1] for e in elements]
        return '/'.join(values)

    def _get_extension_elements(self) -> list[loc_param_value_tuple]:
        """Return a list of (urlName, value) tuples ('bldng' instead of 'bld')."""
        identifiers = []
        for url_name, attr_name in self._url_member_mapping:
            value = getattr(self, attr_name) or ''
            identifiers.append((url_name, value))
        return identifiers

    @property
    def scope_string(self) -> str:
        """Return a string that can be used in ScopesType for discovery."""
        return self._mk_scope_string(self._get_extension_elements())

    def _mk_scope_string(self, elements: Iterable[loc_param_value_tuple]) -> str:
        """Return a string that can be used in ScopeType."""
        # example: sdc.ctxt.loc:/sdc.ctxt.loc.detail/HOSP1%2F%2F%2FCU1%2F%2FBedA500?fac=HOSP1&poc=CU1&bed=BedA500
        identifiers = []
        query_dict = OrderedDict()  # technically an OrderedDict is not necessary here,
                                    # but it is better to keep the Query arguments sorted.
                                    # => easier testing, simple string compare.
        for url_name, value in elements:
            identifiers.append(value)
            if value:
                query_dict[url_name] = value
        identifiers = [quote(ident) for ident in identifiers]
        slash = quote('/', safe='')
        loc = slash.join(identifiers)  # this is a bit ugly, but urllib.quote does not touch slashes;
        query = urlencode(query_dict)
        path = f'/{quote(self.root)}/{loc}'
        return urlunparse(
            ParseResult(scheme=self.scheme, netloc=None, path=path, params=None, query=query, fragment=None))

    def matching_services(self, services: Iterable[Service]) -> list[Service]:
        """Return services that have a scope string element that matches location."""
        return [s for s in services if self._service_matches(s)]

    def _service_matches(self, service: Service) -> bool:
        return self._any_scope_string_matches(service.scopes.text)

    def _any_scope_string_matches(self, scope_texts: list[str]) -> bool:
        return any(self._scope_string_matches(scope) for scope in scope_texts)

    def _scope_string_matches(self, scope_text: str) -> bool:
        """Check if location in scope is inside own location."""
        try:
            other = self.__class__.from_scope_string(str(scope_text))
            return other in self
        except UrlSchemeError:
            # Scope has different scheme, no match
            return False

    def __contains__(self, other: SdcLocation) -> bool:
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
    def from_scope_string(cls, scope_string: str) -> SdcLocation:
        """Construct a Location from a scope string.

        If url scheme is not 'sdc.ctxt.loc', an UrlSchemeError is raised
        :param scope_string: an url
        :return: a SdcLocation object.
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

    def __eq__(self, other: Any) -> bool:
        attr_names = [e[1] for e in self._url_member_mapping]
        attr_names.append('root')
        try:
            return all(getattr(self, attr_name) == getattr(other, attr_name) for attr_name in attr_names)
        except AttributeError:
            return False

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __str__(self) -> str:
        return f'{self.__class__.__name__} {self.scope_string}'
