"""Tests for :mod:`sdc11073.provider.porttypes.invocationsource` (IEEE 11073-20701 R0077/R0078)."""

from __future__ import annotations

import pytest

from sdc11073.provider.porttypes import invocationsource


def _cert(*common_names: str) -> dict:
    """Build a decoded certificate dict as returned by ssl.SSLSocket.getpeercert."""
    subject = [(('countryName', 'DE'),), (('organizationName', 'Test Company'),)]
    subject.extend((('commonName', cn),) for cn in common_names)
    return {'subject': tuple(subject), 'issuer': ()}


class TestGetCommonName:
    def test_none_certificate(self):
        assert invocationsource.get_common_name(None) is None

    def test_empty_certificate(self):
        assert invocationsource.get_common_name({}) is None

    def test_no_common_name(self):
        assert invocationsource.get_common_name({'subject': ((('organizationName', 'Test Company'),),)}) is None

    def test_common_name(self):
        assert invocationsource.get_common_name(_cert('CName')) == 'CName'

    def test_first_common_name_wins(self):
        assert invocationsource.get_common_name(_cert('first', 'second')) == 'first'


class TestMkInvocationSource:
    def test_anonymous_without_certificate(self):
        instance_identifier = invocationsource.mk_invocation_source(None)
        # IEEE Std 11073-20701-2018, 7.2.2, R0077
        assert instance_identifier.Root == 'http://standards.ieee.org/downloads/11073/11073-20701-2018'
        assert instance_identifier.Extension == 'AnonymousSdcParticipant'

    def test_anonymous_with_certificate_without_common_name(self):
        instance_identifier = invocationsource.mk_invocation_source({'subject': ()})
        assert instance_identifier.Extension == 'AnonymousSdcParticipant'

    def test_known_participant(self):
        instance_identifier = invocationsource.mk_invocation_source(_cert('CName'))
        # IEEE Std 11073-20701-2018, 7.2.2, R0078
        assert instance_identifier.Root == (
            'http://standards.ieee.org/downloads/11073/11073-20701-2018/DistinguishedName'
        )
        assert instance_identifier.Extension == 'CName'


@pytest.mark.parametrize('empty', [None, {}, {'subject': ()}])
def test_mk_anonymous_matches_fallback(empty: dict | None):
    assert invocationsource.mk_invocation_source(empty).Root == invocationsource.mk_anonymous_invocation_source().Root
    assert (
        invocationsource.mk_invocation_source(empty).Extension
        == invocationsource.mk_anonymous_invocation_source().Extension
    )
