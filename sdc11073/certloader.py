import ssl
import os

def mk_ssl_context_from_folder(ca_folder,
                               private_key='userkey.pem',
                               certificate='usercert.pem',
                               ca_public_key='cacert.pem',
                               cyphers_file=None,
                               ssl_passwd=None):
    """Convenience method for easy creation of SSL context, assuming all needed files are in the same folder.
    Create an ssl context from files 'userkey.pem', 'usercert.pem', and optional 'cacert.pem' and cyphers file
    :param ca_folder: base path of all files
    :param private_key: name of the private key file of the user
    :param certificate: name of the signed certificate of the user
    :param ca_public_key: name of public key of the certificate authority that signed the certificate; if given,
                   verify_mode of sslContext will be set to CERT_REQUIRED
    :param cyphers_file: optional file that contains a cyphers string; comments are possible, start line with '#'
    :param ssl_passwd: optional password string
    :return: SSLContext instance
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
                if len(cyphers) == 0: # end of file reached without having found a valid line
                    cyphers = None
                    break
                cyphers = cyphers.strip()
                cyphers = cyphers.rstrip('\n')
                cyphers = cyphers.rstrip('\r')
                if len(cyphers) > 0 and not cyphers.startswith("#"):
                    break
    else:
        cyphers = None
    return mk_ssl_context(keyfile, certfile, cafile, cyphers, ssl_passwd)


def mk_ssl_context(key_file,
                   cert_file,
                   ca_file,
                   cyphers=None,
                   ssl_passwd=None):
    """Convenience method for easy creation of SSL context.
    Create an ssl context from files 'userkey.pem', 'usercert.pem', 'cacert.pem' and optional 'cyphers.json'
    :param key_file: the private key pem file of the user
    :param cert_file: the signed certificate of the user
    :param ca_file: optional public key of the certificate authority that signed the certificate; if given,
                   verify_mode of sslContext will be set to CERT_REQUIRED
    :param cyphers: optional cyphers string
    :param ssl_passwd: optional password string
    :return: SSLContext instance
    """
    ssl_context = ssl.SSLContext()  # defaults to ssl.PROTOCOL_TLS
    ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file, password=ssl_passwd)
    if cyphers is not None:
        ssl_context.set_ciphers(cyphers)
    if ca_file:
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(ca_file)
    return ssl_context
