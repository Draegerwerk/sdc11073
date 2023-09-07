from __future__ import annotations

import dataclasses
import pathlib
import ssl
from typing import TYPE_CHECKING, Callable, Union

if TYPE_CHECKING:
    PasswordType = Union[Callable[[], Union[str, bytes]], str, bytes]  # taken from ssl._PasswordType


@dataclasses.dataclass
class SSLContextContainer:
    client_context: ssl.SSLContext
    server_context: ssl.SSLContext


def mk_ssl_contexts_from_folder(ca_folder: str | pathlib.Path,
                                private_key: str = 'userkey.pem',
                                certificate: str = 'usercert.pem',
                                ca_public_key: str | None = 'cacert.pem',
                                cyphers_file: str | pathlib.Path | None = None,
                                ssl_passwd: PasswordType | None = None) -> SSLContextContainer:
    """Convenience method for easy creation of SSL context, assuming all needed files are in the same folder.
    Create an ssl context from files 'userkey.pem', 'usercert.pem', and optional 'cacert.pem' and cyphers file
    :param ca_folder: base path of all files
    :param private_key: name of the private key file of the user
    :param certificate: name of the signed certificate of the user
    :param ca_public_key: name of public key of the certificate authority that signed the certificate; if given,
                   verify_mode of ssl contexts in the return value  will be set to CERT_REQUIRED
    :param cyphers_file: optional file that contains a cyphers string; comments are possible, start line with '#'
    :param ssl_passwd: optional password string
    :return: container of SSLContext instances i.e. client_ssl_context and server_ssl_context.
    """
    ca_folder = pathlib.Path(ca_folder)
    cyphers = None
    if cyphers_file:
        for line in ca_folder.joinpath(cyphers_file).read_text().splitlines():
            raw_cyphers = line.strip()
            # allow comment lines, starting with #
            if len(raw_cyphers) > 0 and not raw_cyphers.startswith("#"):
                cyphers = raw_cyphers
                break
    return mk_ssl_contexts(ca_folder.joinpath(private_key),
                           ca_folder.joinpath(certificate),
                           ca_folder.joinpath(ca_public_key) if ca_public_key else None,
                           cyphers,
                           ssl_passwd)


def mk_ssl_contexts(key_file: pathlib.Path,
                    cert_file: pathlib.Path,
                    ca_file: pathlib.Path | None = None,
                    cyphers: str | None = None,
                    ssl_passwd: PasswordType | None = None) -> SSLContextContainer:
    """Convenience method for easy creation of SSL context.
    Create an ssl context from files 'userkey.pem', 'usercert.pem', 'cacert.pem' and optional 'cyphers.json'
    :param key_file: the private key pem file of the user
    :param cert_file: the signed certificate of the user
    :param ca_file: optional public key of the certificate authority that signed the certificate; if given,
                   verify_mode of ssl contexts in the return value  will be set to CERT_REQUIRED
    :param cyphers: optional cyphers string
    :param ssl_passwd: optional password string
    :return: container of SSLContext instances i.e. client_ssl_context and server_ssl_context.
    """
    if not key_file.exists():
        raise FileNotFoundError(key_file)
    if not cert_file.exists():
        raise FileNotFoundError(cert_file)
    if ca_file and not ca_file.exists():
        raise FileNotFoundError(ca_file)

    client_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    server_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    client_ssl_context.check_hostname = False
    client_ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file, password=ssl_passwd)
    if cyphers is not None:
        client_ssl_context.set_ciphers(cyphers)
    if ca_file:
        client_ssl_context.verify_mode = ssl.CERT_REQUIRED
        client_ssl_context.load_verify_locations(ca_file)

    server_ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file, password=ssl_passwd)
    if cyphers is not None:
        server_ssl_context.set_ciphers(cyphers)
    if ca_file:
        server_ssl_context.verify_mode = ssl.CERT_REQUIRED
        server_ssl_context.load_verify_locations(ca_file)

    return SSLContextContainer(client_context=client_ssl_context, server_context=server_ssl_context)
