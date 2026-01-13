"""HTTP server implementation with threading and request dispatching."""

from __future__ import annotations

import logging
import socket
import threading
from dataclasses import dataclass
from http.server import HTTPServer
from typing import TYPE_CHECKING

from sdc11073.dispatch import PathElementRegistry
from sdc11073.loghelper import LoggerAdapter

from .httprequesthandler import DispatchingRequestHandler

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073 import certloader


@dataclass(frozen=True)
class _ThreadInfo:
    thread: threading.Thread
    request: socket.socket
    client_address: tuple


class _ThreadingHTTPServer(HTTPServer):
    """Each request is handled in a thread."""

    def __init__(
        self,
        logger: LoggerAdapter,
        server_address: tuple[str, int],
        chunk_size: int,
        supported_encodings: Iterable[str],
    ):
        self.daemon_threads = True
        self.threads = []
        self.logger = logger
        self.dispatcher = PathElementRegistry()
        self.chunk_size = chunk_size
        self.supported_encodings = supported_encodings
        super().__init__(server_address, DispatchingRequestHandler)

    def process_request_thread(self, request, client_address):  # noqa: ANN001
        """Same as in BaseServer but as a thread."""  # noqa: D401
        try:
            self.finish_request(request, client_address)
        except (ConnectionResetError, ConnectionAbortedError) as ex:
            self.logger.info('Connection reset by %s: %s', client_address, ex)
        except Exception:  # noqa: BLE001
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)
            # this thread will close after return from this method, it can already be removed from self.threads
            for thread_info in self.threads:
                if thread_info.request == request:
                    self.threads.remove(thread_info)

    def process_request(self, request, client_address):  # noqa: ANN001
        """Start a new thread to process the request."""
        thread = threading.Thread(
            target=self.process_request_thread, args=(request, client_address), name=f'SubscrRecv{client_address}',
        )
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
                    self.logger.info('closed socket for notifications from %s', thread_info.client_address)
                except OSError:
                    # the connection is already closed
                    continue
                except Exception as ex:  # noqa: BLE001
                    self.logger.warning(
                        'error closing socket for notifications from %s: %s', thread_info.client_address, ex,
                    )


class HttpServerThreadBase(threading.Thread):
    """A Thread running a ThreadingHTTPServer."""

    def __init__(
        self,
        my_ipaddress: str,
        ssl_context: certloader.SSLContextContainer | None,
        supported_encodings: Iterable[str],
        logger: logging.Logger | LoggerAdapter,
        chunk_size: int = 0,
    ):
        """Run a ThreadingHTTPServer in a thread, so that it can be stopped without blocking.

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
        self.started_evt = threading.Event()  # helps to wait until thread has initialized is variables
        self._stop_requested = False
        self.base_url = None

    def run(self):
        """Run the http server."""
        self._stop_requested = False
        try:
            myport = 0  # zero means that OS selects a free port
            self.httpd = _ThreadingHTTPServer(
                self.logger, (self._my_ipaddress, myport), self.chunk_size, self.supported_encodings,
            )
            self.my_port = self.httpd.server_port
            self.logger.info('starting http server on %s:%s', self._my_ipaddress, self.my_port)
            if self._ssl_context:
                self.httpd.socket = self._ssl_context.wrap_socket(self.httpd.socket, server_side=True)
                self.base_url = f'https://{self._my_ipaddress}:{self.my_port}/'
            else:
                self.base_url = f'http://{self._my_ipaddress}:{self.my_port}/'

            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            if not self._stop_requested:
                self.logger.exception('Unhandled Exception at thread runtime. Thread will abort!')
            raise
        finally:
            self.logger.info('http server stopped.')

    @property
    def dispatcher(self) -> PathElementRegistry:
        """Return the dispatcher responsible for handling requests."""
        if not self.started_evt.is_set():
            raise RuntimeError('http server not started yet, dispatcher not available')
        return self.httpd.dispatcher

    def stop(self):
        """Stop the http server."""
        if not self.started_evt.is_set():
            raise RuntimeError('http server was not started yet')
        self._stop_requested = True
        self.httpd.shutdown()
        self.httpd.server_close()
        for thread_info in self.httpd.threads:
            if thread_info.thread.is_alive():
                thread_info.thread.join(1)
            if thread_info.thread.is_alive():
                self.logger.warning('could not end client thread for notifications from %s', thread_info.client_address)
        del self.httpd.threads[:]
        self.started_evt.clear()
