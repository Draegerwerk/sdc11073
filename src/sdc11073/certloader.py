import dataclasses
import os
import ssl


@dataclasses.dataclass
class SSLContextContainer:
    client_context: ssl.SSLContext
    server_context: ssl.SSLContext


def mk_ssl_contexts_from_folder(ca_folder,
                                private_key='userkey.pem',
                                certificate='usercert.pem',
                                ca_public_key='cacert.pem',
                                cyphers_file=None,
                                ssl_passwd=None) -> SSLContextContainer:
    """Convenience method for easy creation of SSL context, assuming all needed files are in the same folder.
    Create an ssl context from files 'userkey.pem', 'usercert.pem', and optional 'cacert.pem' and cyphers file
    :param ca_folder: base path of all files
    :param private_key: name of the private key file of the user
    :param certificate: name of the signed certificate of the user
    :param ca_public_key: name of public key of the certificate authority that signed the certificate; if given,
                          verify_mode of ssl contexts in the return value will be set to CERT_REQUIRED
    :param cyphers_file: optional file that contains a cyphers string; comments are possible, start line with '#'
    :param ssl_passwd: optional password string
    :return: container of SSLContext instances i.e. client_ssl_context and server_ssl_context
    """
    certfile = os.path.join(ca_folder, certificate)
    keyfile = os.path.join(ca_folder, private_key)
    if ca_public_key:
        cafile = os.path.join(ca_folder, ca_public_key)
    else:
        cafile = None
    if cyphers_file:
        with open(os.path.join(ca_folder, cyphers_file)) as f:
            while True:
                # allow comment lines, starting with #
                cyphers = f.readline()
                if len(cyphers) == 0:  # end of file reached without having found a valid line
                    cyphers = None
                    break
                cyphers = cyphers.strip()
                cyphers = cyphers.rstrip('\n')
                cyphers = cyphers.rstrip('\r')
                if len(cyphers) > 0 and not cyphers.startswith("#"):
                    break
    else:
        cyphers = None
    return mk_ssl_contexts(keyfile, certfile, cafile, cyphers, ssl_passwd)


def mk_ssl_contexts(key_file,
                    cert_file,
                    ca_file,
                    cyphers=None,
                    ssl_passwd=None) -> SSLContextContainer:
    """Convenience method for easy creation of SSL context.
    Create an ssl context from files 'userkey.pem', 'usercert.pem', 'cacert.pem' and optional 'cyphers.json'
    :param key_file: the private key pem file of the user
    :param cert_file: the signed certificate of the user
    :param ca_file: optional public key of the certificate authority that signed the certificate; if given,
                    verify_mode of ssl contexts in the return value will be set to CERT_REQUIRED
    :param cyphers: optional cyphers string
    :param ssl_passwd: optional password string
    :return: container of SSLContext instances i.e. client_ssl_context and server_ssl_context
    """
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
