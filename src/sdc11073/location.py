from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import ParseResult, parse_qsl, quote, unquote, urlencode, urlsplit, urlunparse

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073.wsdiscovery.service import Service


class UrlSchemeError(Exception):
    """Indicate that the scheme is wrong."""


class SdcLocation:
    """SdcLocation contains all parameters that define a location.

    It serves two purposes:
    1. it can create a scope string for discovery.
    2. it can filter services that are located "inside" this location (this is different from discovery matching!).
        E.g. if this location has only facility and building set, then all services with the same facility and
        building are considered "inside".
        Hierarchy is 'fac', 'bldng', 'flr', 'poc', 'rm', 'bed'.
        It is debatable if a floor contains 1...n points of care or a point of care contains 1...n floors,
        so be careful with this!
    """

    scheme = 'sdc.ctxt.loc'  # constant for scheme from sdc standard

    url_elements = ('fac', 'bldng', 'flr', 'poc', 'rm', 'bed')  # order also defines hierarchy

    def __init__(self,  # noqa: PLR0913
                 fac: str | None = None,
                 poc: str | None = None,
                 bed: str | None = None,
                 bldng: str | None = None,
                 flr: str | None = None,
                 rm: str | None = None,
                 root: str = 'sdc.ctxt.loc.detail'):
        self.root = root
        self.fac = fac  # facility
        self.bldng = bldng  # building
        self.poc = poc  # point of care
        self.flr = flr  # floor
        self.rm = rm  # room
        self.bed = bed  # Bed

    @property
    def scope_string(self) -> str:
        """Return a string that can be used in ScopesType for discovery.

        Example: sdc.ctxt.loc:/sdc.ctxt.loc.detail/HOSP1%2F%2F%2FCU1%2F%2FBed42?fac=HOSP1&poc=CU1&bed=Bed42
        """
        identifiers = []
        query_dict = {}
        for url_name in self.url_elements:
            value = getattr(self, url_name) or ''  # None value becomes an empty string.
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

    def filter_services_inside(self, services: Iterable[Service]) -> list[Service]:
        """Return services that are 'inside' own location (see doc string of class)."""
        return [s for s in services if self._service_matches(s)]

    def _service_matches(self, service: Service) -> bool:
        if service.scopes is None:
            return False
        return any(self._scope_string_matches(scope) for scope in service.scopes.text)

    def _scope_string_matches(self, scope_text: str) -> bool:
        """Check if location in scope is inside own location."""
        try:
            other = self.__class__.from_scope_string(scope_text)
            return other in self
        except UrlSchemeError:
            # Scope has different scheme, no match
            return False

    def __contains__(self, other: SdcLocation) -> bool:
        """Compare element by element, root included.

        If own element is None, every value of other is accepted, else they must be identical.
        """
        if self.root != other.root:
            return False
        for attr_name in self.url_elements:
            my_attr = getattr(self, attr_name)
            if my_attr is not None:
                if my_attr != getattr(other, attr_name):
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
        # make a new argumentsDict with well known keys.
        # This allows to ignore unknown keys that might be present in query_dict
        arguments_dict = {}
        for attr_name in cls.url_elements:
            arguments_dict[attr_name] = query_dict.get(attr_name)

        arguments_dict['root'] = root
        return cls(**arguments_dict)

    def __eq__(self, other: object) -> bool:
        attr_names = (*self.url_elements, 'root')
        try:
            return all(getattr(self, attr_name) == getattr(other, attr_name) for attr_name in attr_names)
        except AttributeError:
            return False

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __str__(self) -> str:
        return f'{self.__class__.__name__} {self.scope_string}'
