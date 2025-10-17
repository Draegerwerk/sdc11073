"""Common utilities and configuration helpers for ReferenceTestV2."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.certloader import mk_ssl_contexts_from_folder

if TYPE_CHECKING:
    import pathlib

    import sdc11073


def get_ssl_context(folder: pathlib.Path, password: str | None) -> sdc11073.certloader.SSLContextContainer:
    """Create an SSL context based on the active configuration."""
    return mk_ssl_contexts_from_folder(
        ca_folder=folder,
        private_key='user_private_key_encrypted.pem',
        certificate='user_certificate_root_signed.pem',
        ca_public_key='root_certificate.pem',
        cyphers_file=None,
        ssl_passwd=password,
    )
