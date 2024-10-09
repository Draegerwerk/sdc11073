from __future__ import annotations

import logging
import socket
import threading
import traceback
from dataclasses import dataclass
from http.server import HTTPServer

from .httprequesthandler import DispatchingRequestHandler
from sdc11073.dispatch import PathElementRegistry
from sdc11073.loghelper import LoggerAdapter


@dataclass(frozen=True)
class _ThreadInfo:
    thread: threading.Thread
    request: socket.socket
    client_address: tuple


class _ThreadingHTTPServer(HTTPServer):
    """ Each request is handled in a thread.
    """

    def __init__(self, logger, server_address,
                 chunk_size, supported_encodings):
        self.daemon_threads = True
        self.threads = []
        self.logger = logger
        self.dispatcher = PathElementRegistry()
        self.chunk_size = chunk_size
        self.supported_encodings = supported_encodings
        super().__init__(server_address, DispatchingRequestHandler)

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.
        """
        try:
            self.finish_request(request, client_address)
        except (ConnectionResetError, ConnectionAbortedError) as ex:
            self.logger.info('Connection reset by {}: {}', client_address, ex)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)
            # this thread will close after return from this method, it can already be removed from self.threads
            for thread_info in self.threads:
                if thread_info.request == request:
                    self.threads.remove(thread_info)

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        thread = threading.Thread(target=self.process_request_thread,
                                  args=(request, client_address),
                                  name=f'SubscrRecv{client_address}')
        thread.daemon = True
        self.threads.append(_ThreadInfo(thread, request, client_address))
        thread.start()

    def server_close(self):
        super().server_close()
        if self.dispatcher is not None:
            self.dispatcher.methods = {}
            self.dispatcher = None  # this leads to a '503' reaction in SOAPNotificationsHandler
        for thread_info in self.threads:
            if thread_info.thread.is_alive():
                try:
                    thread_info.request.shutdown(socket.SHUT_RDWR)
                    thread_info.request.close()
                    self.logger.info('closed socket for notifications from {}', thread_info.client_address)
                except OSError:
                    # the connection is already closed
                    continue
                except Exception as ex:
                    self.logger.warn('error closing socket for notifications from {}: {}', thread_info.client_address,
                                     ex)


class HttpServerThreadBase(threading.Thread):

    def __init__(self, my_ipaddress, ssl_context, supported_encodings,
                 logger, chunk_size=0):
        """
        Runs a ThreadingHTTPServer in a thread, so that it can be stopped without blocking.
        Handling of requests happens in two stages:
        - the http server instantiates a request handler with the request
        - the request handler forwards the handling itself to a dispatcher (due to the dynamic nature of the handling).
        :param my_ipaddress: The ip address that the http server shall bind to (no port!)
        :param ssl_context: a ssl.SslContext instance or None
        :param supported_encodings: a list of strings
        :param logger: a python logger
        :param chunk_size: if value > 0, messages are split into chunks of this size.
        """
        super().__init__(name='Dev_SdcHttpServerThread')
        self.daemon = True

        self._my_ipaddress = my_ipaddress
        self._ssl_context = ssl_context
        self.my_port = None
        self.httpd = None
        self.supported_encodings = supported_encodings
        if isinstance(logger, logging.Logger):
            self.logger = LoggerAdapter(logger)
        else:
            self.logger = logger
        self.chunk_size = chunk_size
        # create and set up the dispatcher for all incoming requests
        self.started_evt = threading.Event()  # helps to wait until thread has initialised is variables
        self._stop_requested = False
        self.base_url = None

    def run(self):
        self._stop_requested = False
        try:
            myport = 0  # zero means that OS selects a free port
            self.httpd = _ThreadingHTTPServer(self.logger,
                                              (self._my_ipaddress, myport),
                                              self.chunk_size,
                                              self.supported_encodings)
            self.my_port = self.httpd.server_port
            self.logger.info('starting http server on {}:{}', self._my_ipaddress, self.my_port)
            if self._ssl_context:
                self.httpd.socket = self._ssl_context.wrap_socket(self.httpd.socket, server_side=True)
                self.base_url = f'https://{self._my_ipaddress}:{self.my_port}/'
            else:
                self.base_url = f'http://{self._my_ipaddress}:{self.my_port}/'

            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            if not self._stop_requested:
                self.logger.error(
                    f'Unhandled Exception at thread runtime. Thread will abort! {traceback.format_exc()}')
            raise
        finally:
            self.logger.info('http server stopped.')

    @property
    def dispatcher(self):
        return self.httpd.dispatcher

    def stop(self):
        self._stop_requested = True
        self.httpd.shutdown()
        self.httpd.server_close()
        for thread_info in self.httpd.threads:
            if thread_info.thread.is_alive():
                thread_info.thread.join(1)
            if thread_info.thread.is_alive():
                self.logger.warn('could not end client thread for notifications from {}', thread_info.client_address)
        del self.httpd.threads[:]
