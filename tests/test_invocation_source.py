"""Tests for :mod:`sdc11073.provider.invocationsource` (IEEE 11073-20701 R0077/R0078)."""

from __future__ import annotations

import datetime as dt
import pathlib
import ssl
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from sdc11073.provider import invocationsource
from sdc11073.provider.providerimpl import provider_components_async_factory, provider_components_sync_factory

if TYPE_CHECKING:
    from collections.abc import Callable

    from sdc11073.provider.providerimpl import SdcProviderComponents

# self-signed v1 certificate (without version field) with the subject Common Name 'CName', used by the tls tests
_TEST_CERTIFICATE_PEM = pathlib.Path(__file__).parent.joinpath('certificates', 'test_certificate.pem').read_text()
_TEST_CERTIFICATE = ssl.PEM_cert_to_DER_cert(_TEST_CERTIFICATE_PEM)
_KEY = ec.generate_private_key(ec.SECP256R1())


def _rdn(*attributes: tuple[x509.ObjectIdentifier, str]) -> x509.RelativeDistinguishedName:
    """Build a relative distinguished name of the given (oid, value) attributes."""
    return x509.RelativeDistinguishedName([x509.NameAttribute(oid, value) for oid, value in attributes])


def _mk_certificate(*subject: x509.RelativeDistinguishedName) -> bytes:
    """Build a self-signed DER encoded x.509 certificate with the given subject."""
    name = x509.Name(subject)
    now = dt.datetime.now(dt.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(_KEY.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + dt.timedelta(days=1))
        .sign(_KEY, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.DER)


@pytest.fixture
def without_cryptography(monkeypatch: pytest.MonkeyPatch):
    """Behave as if the optional dependency cryptography is not installed."""
    monkeypatch.setattr(invocationsource, '_CRYPTOGRAPHY_AVAILABLE', False)


def test_provider_module_without_cryptography():
    script = """
import sys
sys.modules['cryptography'] = None

from sdc11073.provider.invocationsource import (
    get_common_names,
    mk_common_name_invocation_source,
    mk_pem_certificate_invocation_source,
)

pem = mk_pem_certificate_invocation_source(b'test')
assert pem.Root == 'http://standards.ieee.org/downloads/11073/11073-20701-2018/X509Certificate/PEM'
assert 'dGVzdA==' in pem.Extension
assert mk_pem_certificate_invocation_source(None).Extension == 'AnonymousSdcParticipant'
for invoke in (lambda: get_common_names(b'test'), lambda: mk_common_name_invocation_source(None)):
    try:
        invoke()
    except ImportError as exc:
        assert exc.name == 'cryptography'
        assert 'pip install sdc11073[cryptography]' in str(exc)
    else:
        raise AssertionError('Common Name identification should require cryptography')
"""
    result = subprocess.run([sys.executable, '-c', script], capture_output=True, check=False, text=True)  # noqa: S603
    assert result.returncode == 0, result.stderr


class TestGetCommonNames:
    def test_test_certificate(self):
        assert invocationsource.get_common_names(_TEST_CERTIFICATE) == ['CName']

    def test_common_names_in_order_of_distinguished_name(self):
        certificate = _mk_certificate(
            _rdn((NameOID.ORGANIZATION_NAME, 'Test Company')),
            _rdn((NameOID.COMMON_NAME, 'first')),
            _rdn((NameOID.ORGANIZATIONAL_UNIT_NAME, 'Unit'), (NameOID.COMMON_NAME, 'second')),
            _rdn((NameOID.COMMON_NAME, 'thïrd')),
        )
        assert invocationsource.get_common_names(certificate) == ['first', 'second', 'thïrd']

    def test_no_common_name(self):
        certificate = _mk_certificate(_rdn((NameOID.ORGANIZATION_NAME, 'Test Company')))
        assert invocationsource.get_common_names(certificate) == []

    @pytest.mark.parametrize(
        'data',
        [b'', b'\x30', b'no certificate', _TEST_CERTIFICATE[:-1]],
        ids=['empty', 'missing_length', 'no_der', 'truncated_certificate'],
    )
    def test_invalid_certificate(self, data: bytes):
        with pytest.raises(ValueError):  # noqa: PT011 - the message is defined by cryptography
            invocationsource.get_common_names(data)

    @pytest.mark.usefixtures('without_cryptography')
    def test_without_cryptography(self):
        with pytest.raises(ImportError, match=r'pip install sdc11073\[cryptography\]'):
            invocationsource.get_common_names(_TEST_CERTIFICATE)


class TestMkCommonNameInvocationSource:
    def test_known_participant(self):
        instance_identifier = invocationsource.mk_common_name_invocation_source(_TEST_CERTIFICATE)
        # IEEE Std 11073-20701-2018, 7.2.2, R0078
        assert instance_identifier.Root == (
            'http://standards.ieee.org/downloads/11073/11073-20701-2018/DistinguishedName'
        )
        assert instance_identifier.Extension == 'CName'

    def test_most_specific_common_name(self):
        certificate = _mk_certificate(_rdn((NameOID.COMMON_NAME, 'first')), _rdn((NameOID.COMMON_NAME, 'last')))
        assert invocationsource.mk_common_name_invocation_source(certificate).Extension == 'last'

    @pytest.mark.parametrize(
        'certificate',
        [None, _mk_certificate(_rdn((NameOID.ORGANIZATION_NAME, 'Test Company')))],
        ids=['no_certificate', 'no_common_name'],
    )
    def test_anonymous_participant(self, certificate: bytes | None):
        instance_identifier = invocationsource.mk_common_name_invocation_source(certificate)
        # IEEE Std 11073-20701-2018, 7.2.2, R0077
        assert instance_identifier.Root == 'http://standards.ieee.org/downloads/11073/11073-20701-2018'
        assert instance_identifier.Extension == 'AnonymousSdcParticipant'

    @pytest.mark.usefixtures('without_cryptography')
    @pytest.mark.parametrize('certificate', [None, _TEST_CERTIFICATE], ids=['no_certificate', 'certificate'])
    def test_without_cryptography(self, certificate: bytes | None):
        with pytest.raises(ImportError, match=r'pip install sdc11073\[cryptography\]') as exc_info:
            invocationsource.mk_common_name_invocation_source(certificate)
        assert exc_info.value.name == 'cryptography'


class TestMkPemCertificateInvocationSource:
    def test_known_participant(self):
        instance_identifier = invocationsource.mk_pem_certificate_invocation_source(_TEST_CERTIFICATE)
        assert instance_identifier.Root == (
            'http://standards.ieee.org/downloads/11073/11073-20701-2018/X509Certificate/PEM'
        )
        assert instance_identifier.Extension == _TEST_CERTIFICATE_PEM

    def test_anonymous_participant(self):
        instance_identifier = invocationsource.mk_pem_certificate_invocation_source(None)
        assert instance_identifier.Root == invocationsource.ANONYMOUS_PARTICIPANT_ROOT
        assert instance_identifier.Extension == invocationsource.ANONYMOUS_PARTICIPANT_EXTENSION

    @pytest.mark.usefixtures('without_cryptography')
    def test_without_cryptography(self):
        instance_identifier = invocationsource.mk_pem_certificate_invocation_source(_TEST_CERTIFICATE)
        assert instance_identifier.Extension == _TEST_CERTIFICATE_PEM


@pytest.mark.parametrize('components_factory', [provider_components_sync_factory, provider_components_async_factory])
def test_default_invocation_source_factory(components_factory: Callable[[], SdcProviderComponents]):
    """The PEM encoded certificate is the default, as it does not need the optional dependency cryptography."""
    components = components_factory()
    assert components.invocation_source_factory is invocationsource.mk_pem_certificate_invocation_source
