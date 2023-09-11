import io
import logging
import os
import tempfile
import unittest
import uuid

from sdc11073 import commlog


class TestCommLogger(unittest.TestCase):

    def test_stream(self):
        """Test the comm stream logger."""
        stream = io.StringIO()
        with commlog.StreamLogger(stream=stream):
            self.assertEqual(stream.getvalue(), '')

            for name in commlog.LOGGER_NAMES:
                logger = logging.getLogger(name)
                self.assertEqual(1, len(logger.handlers))
                self.assertIsInstance(logger.handlers[0], logging.StreamHandler)
                message = str(uuid.uuid4())
                logger.debug(message)
                self.assertIn(message, stream.getvalue())
                stream.truncate(0)

        for name in commlog.LOGGER_NAMES:
            self.assertEqual(0, len(logging.getLogger(name).handlers))

    def test_broadcast_filter(self):
        """Test the comm stream logger."""
        stream = io.StringIO()
        broadcast_ip_filter = 'some ip address of users choice'
        with commlog.StreamLogger(stream=stream, broadcast_ip_filter=broadcast_ip_filter):
            self.assertEqual(stream.getvalue(), '')

            for name in (commlog.DISCOVERY_IN, commlog.DISCOVERY_OUT):
                logger = logging.getLogger(name)
                self.assertEqual(1, len(logger.handlers))
                self.assertEqual(1, len(logger.handlers[0].filters))
                self.assertIsInstance(logger.handlers[0].filters[0], commlog.IpFilter)
                message = str(uuid.uuid4())
                logger.debug(message, extra={'ip_address': broadcast_ip_filter})
                self.assertIn(message, stream.getvalue())
                stream.truncate(0)
                logger.debug(message, extra={'ip_address': str(uuid.uuid4())})
                self.assertNotIn(message, stream.getvalue())
                stream.truncate(0)
                logger.debug(message)
                self.assertNotIn(message, stream.getvalue())
                stream.truncate(0)

        for name in commlog.LOGGER_NAMES:
            self.assertEqual(0, len(logging.getLogger(name).handlers))

    def test_directory_direction_in(self):
        """Test the comm directory logger in direction."""
        with tempfile.TemporaryDirectory() as directory, commlog.DirectoryLogger(log_folder=directory, log_in=True):
            self.assertEqual(0, len(os.listdir(directory)))
            for i, name in enumerate((commlog.DISCOVERY_IN,
                                      commlog.SOAP_REQUEST_IN,
                                      commlog.SOAP_RESPONSE_IN,
                                      commlog.SOAP_SUBSCRIPTION_IN,
                                      commlog.WSDL), start=1):
                ip_address = uuid.uuid4().hex
                http_method = uuid.uuid4().hex
                logging.getLogger(name).debug(str(uuid.uuid4()),
                                              extra={'ip_address': ip_address, 'http_method': http_method})
                self.assertEqual(i, len(os.listdir(directory)))
                self.assertEqual(1, len([file for file in os.listdir(directory)
                                         if ip_address in file and http_method in file]))

            for name in (commlog.MULTICAST_OUT,
                         commlog.DISCOVERY_OUT,
                         commlog.SOAP_REQUEST_OUT,
                         commlog.SOAP_RESPONSE_OUT):
                logging.getLogger(name).debug(str(uuid.uuid4()))
                self.assertEqual(i, len(os.listdir(directory)))

    def test_directory_direction_out(self):
        """Test the comm directory logger out direction."""
        with tempfile.TemporaryDirectory() as directory, commlog.DirectoryLogger(log_folder=directory, log_out=True):
            self.assertEqual(0, len(os.listdir(directory)))
            for i, name in enumerate((commlog.MULTICAST_OUT,
                                      commlog.DISCOVERY_OUT,
                                      commlog.SOAP_REQUEST_OUT,
                                      commlog.SOAP_RESPONSE_OUT),
                                     start=1):
                ip_address = uuid.uuid4().hex
                http_method = uuid.uuid4().hex
                logging.getLogger(name).debug(str(uuid.uuid4()),
                                              extra={'ip_address': ip_address, 'http_method': http_method})
                self.assertEqual(i, len(os.listdir(directory)))
                self.assertEqual(1, len([file for file in os.listdir(directory)
                                         if ip_address in file and http_method in file]))

            for name in (commlog.DISCOVERY_IN,
                         commlog.SOAP_REQUEST_IN,
                         commlog.SOAP_RESPONSE_IN,
                         commlog.SOAP_SUBSCRIPTION_IN,
                         commlog.WSDL):
                logging.getLogger(name).debug(str(uuid.uuid4()))
                self.assertEqual(i, len(os.listdir(directory)))

    def test_broadcast_filter_directory_logger(self):
        """Test the comm directory logger broadcast filter."""
        broadcast_ip_filter = uuid.uuid4().hex
        with (tempfile.TemporaryDirectory() as directory,
              commlog.DirectoryLogger(log_folder=directory,
                                      log_out=True,
                                      log_in=True,
                                      broadcast_ip_filter=broadcast_ip_filter)):
            self.assertEqual(0, len(os.listdir(directory)))

            for i, name in enumerate((commlog.DISCOVERY_IN, commlog.DISCOVERY_OUT), start=1):
                logger = logging.getLogger(name)
                self.assertEqual(1, len(logger.handlers))
                self.assertEqual(1, len(logger.handlers[0].filters))
                self.assertIsInstance(logger.handlers[0].filters[0], commlog.IpFilter)
                message = str(uuid.uuid4())
                logger.debug(message, extra={'ip_address': str(uuid.uuid4())})
                self.assertEqual(i - 1, len(os.listdir(directory)))
                logger.debug(message)
                self.assertEqual(i - 1, len(os.listdir(directory)))
                logger.debug(message, extra={'ip_address': broadcast_ip_filter})
                self.assertEqual(i, len(os.listdir(directory)))

        for name in commlog.LOGGER_NAMES:
            self.assertEqual(0, len(logging.getLogger(name).handlers))
