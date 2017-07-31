# -*- coding: utf-8 -*-

import unittest
import os

from openprocurement.bot.identification.tests.utils import custom_sleep
from requests import RequestException

from mock import patch, MagicMock
from restkit import RequestError, ResourceError

from gevent.pywsgi import WSGIServer
from bottle import Bottle, response, request

from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.databridge.utils import check_412


config = {
    'main':
        {
            'tenders_api_server': 'http://127.0.0.1:20604',
            'tenders_api_version': '0',
            'public_tenders_api_server': 'http://127.0.0.1:20605',
            'doc_service_server': 'http://127.0.0.1',
            'doc_service_port': 20606,
            'doc_service_user': 'broker',
            'doc_service_password': 'broker_pass',
            'proxy_server': 'http://127.0.0.1',
            'proxy_port': 20607,
            'proxy_user': 'robot',
            'proxy_password': 'robot',
            'proxy_version': 1.0,
            'buffers_size': 450,
            'full_stack_sync_delay': 15,
            'empty_stack_sync_delay': 101,
            'on_error_sleep_delay': 5,
            'api_token': "api_token"
        }
}


class AlmostAlwaysTrue(object):

    def __init__(self, total_iterations=1):
        self.total_iterations = total_iterations
        self.current_iteration = 0

    def __nonzero__(self):
        if self.current_iteration < self.total_iterations:
            self.current_iteration += 1
            return bool(1)
        return bool(0)


class BaseServersTest(unittest.TestCase):
    """Api server to test openprocurement.integrations.edr.databridge.bridge """

    relative_to = os.path.dirname(__file__)  # crafty line

    @classmethod
    def setUpClass(cls):
        cls.api_server_bottle = Bottle()
        cls.proxy_server_bottle = Bottle()
        cls.doc_server_bottle = Bottle()
        cls.api_server = WSGIServer(('127.0.0.1', 20604), cls.api_server_bottle, log=None)
        setup_routing(cls.api_server_bottle, response_spore)
        cls.public_api_server = WSGIServer(('127.0.0.1', 20605), cls.api_server_bottle, log=None)
        cls.doc_server = WSGIServer(('127.0.0.1', 20606), cls.doc_server_bottle, log=None)
        setup_routing(cls.doc_server_bottle, doc_response, path='/')
        cls.proxy_server = WSGIServer(('127.0.0.1', 20607), cls.proxy_server_bottle, log=None)
        setup_routing(cls.proxy_server_bottle, proxy_response, path='/api/1.0/health')

        # start servers
        cls.api_server.start()
        cls.public_api_server.start()
        cls.doc_server.start()
        cls.proxy_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.api_server.close()
        cls.public_api_server.close()
        cls.doc_server.close()
        cls.proxy_server.close()

    def tearDown(self):
        del self.worker


def setup_routing(app, func, path='/api/0/spore', method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                                      "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                                      "96a11693fa8bd38623e4daee121f60b4301aef012c"))
    return response


def doc_response():
    return response


def proxy_response():
    if request.headers.get("sandbox-mode") != "True":  # Imitation of health comparison
        response.status = 400
    return response


class TestBridgeWorker(BaseServersTest):

    def test_init(self):
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.delay, 15)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)

        # check clients
        self.assertTrue(isinstance(self.worker.tenders_sync_client, TendersClientSync))
        self.assertTrue(isinstance(self.worker.client, TendersClient))
        self.assertTrue(isinstance(self.worker.proxyClient, ProxyClient))
        self.assertTrue(isinstance(self.worker.doc_service_client, DocServiceClient))
        self.assertFalse(self.worker.initialization_event.is_set())
        self.assertEqual(self.worker.process_tracker.processing_items, {})

    @patch('openprocurement.bot.identification.databridge.bridge.ProxyClient')
    @patch('openprocurement.bot.identification.databridge.bridge.DocServiceClient')
    @patch('openprocurement.bot.identification.databridge.bridge.TendersClientSync')
    @patch('openprocurement.bot.identification.databridge.bridge.TendersClient')
    def test_tender_sync_clients(self, sync_client, client, doc_service_client, proxy_client):
        self.worker = EdrDataBridge(config)
        # check client config
        self.assertEqual(client.call_args[0], ('',))
        self.assertEqual(client.call_args[1], {'host_url': config['main']['public_tenders_api_server'],
                                               'api_version': config['main']['tenders_api_version']})

        # check sync client config
        self.assertEqual(sync_client.call_args[0], (config['main']['api_token'],))
        self.assertEqual(sync_client.call_args[1],
                         {'host_url': config['main']['tenders_api_server'],
                          'api_version': config['main']['tenders_api_version']})

        # check doc_service_client config
        self.assertEqual(doc_service_client.call_args[1],
                         {'host': config['main']['doc_service_server'],
                          'port': config['main']['doc_service_port'],
                          'user': config['main']['doc_service_user'],
                          'password': config['main']['doc_service_password']})
        # check proxy_client config
        self.assertEqual(proxy_client.call_args[1],
                         {'host': config['main']['proxy_server'],
                          'port': config['main']['proxy_port'],
                          'user': config['main']['proxy_user'],
                          'password': config['main']['proxy_password'],
                          'version': config['main']['proxy_version']})

    def test_start_jobs(self):
        self.worker = EdrDataBridge(config)

        scanner, filter_tender, edr_handler, upload_file = [MagicMock(return_value=i) for i in range(4)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file

        self.worker._start_jobs()
        # check that all jobs were started
        self.assertTrue(scanner.called)
        self.assertTrue(filter_tender.called)
        self.assertTrue(edr_handler.called)
        self.assertTrue(upload_file.called)

        self.assertEqual(self.worker.jobs['scanner'], 0)
        self.assertEqual(self.worker.jobs['filter_tender'], 1)
        self.assertEqual(self.worker.jobs['edr_handler'], 2)
        self.assertEqual(self.worker.jobs['upload_file'], 3)

    @patch('gevent.sleep')
    def test_run(self, sleep):
        self.worker = EdrDataBridge(config)
        # create mocks
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        with patch('__builtin__.True', AlmostAlwaysTrue(100)):
            self.worker.run()
        self.assertEqual(self.worker.scanner.call_count, 1)
        self.assertEqual(self.worker.filter_tender.call_count, 1)
        self.assertEqual(self.worker.edr_handler.call_count, 1)
        self.assertEqual(self.worker.upload_file.call_count, 1)

    def test_proxy_server(self):
        self.worker = EdrDataBridge(config)
        self.worker.sandbox_mode = "True"
        self.proxy_server.stop()
        with self.assertRaises(RequestException):
            self.worker.check_proxy()
        self.proxy_server.start()
        self.assertTrue(self.worker.check_proxy())

    def test_proxy_server_mock(self):
        self.worker = EdrDataBridge(config)
        self.worker.proxyClient = MagicMock(health=MagicMock(side_effect=RequestError()))
        with self.assertRaises(RequestError):
            self.worker.check_proxy()
        self.worker.proxyClient = MagicMock(return_value=True)
        self.assertTrue(self.worker.check_proxy())

    def test_proxy_server_success(self):
        self.worker = EdrDataBridge(config)
        self.worker.sandbox_mode = "True"
        self.assertTrue(self.worker.check_proxy())

    def test_proxy_sandmox_mismatch(self):
        self.worker = EdrDataBridge(config)
        self.worker.sandbox_mode = "False"
        with self.assertRaises(RequestException):
            self.worker.check_proxy()
        self.worker.sandbox_mode = "True"
        self.assertTrue(self.worker.check_proxy())

    def test_doc_service(self):
        self.doc_server.stop()
        self.worker = EdrDataBridge(config)
        with self.assertRaises(RequestError):
            self.worker.check_doc_service()
        self.doc_server.start()
        self.assertTrue(self.worker.check_doc_service())

    def test_doc_service_mock(self):
        self.worker = EdrDataBridge(config)
        with patch("openprocurement.bot.identification.databridge.bridge.request", side_effect=RequestError()):
            with self.assertRaises(RequestError):
                self.worker.check_doc_service()
        with patch("openprocurement.bot.identification.databridge.bridge.request", return_value=True):
            self.assertTrue(self.worker.check_doc_service())

    def test_openprocurement_api_failure(self):
        self.worker = EdrDataBridge(config)
        self.api_server.stop()
        with self.assertRaises(RequestError):
            self.worker.check_openprocurement_api()
        self.api_server.start()
        self.assertTrue(self.worker.check_openprocurement_api())

    def test_openprocurement_api_mock(self):
        self.worker = EdrDataBridge(config)
        self.worker.client = MagicMock(head=MagicMock(side_effect=RequestError()))
        with self.assertRaises(RequestError):
            self.worker.check_openprocurement_api()
        self.worker.client = MagicMock()
        self.assertTrue(self.worker.check_openprocurement_api())

    def test_check_services(self):
        t = os.environ.get("SANDBOX_MODE", "False")
        os.environ["SANDBOX_MODE"] = "True"
        self.worker = EdrDataBridge(config)
        self.worker.services_not_available = MagicMock(set=MagicMock(), clear=MagicMock())
        self.proxy_server.stop()
        self.worker.check_services()
        self.assertTrue(self.worker.services_not_available.clear.called)
        self.proxy_server.start()
        self.worker.check_services()
        self.assertTrue(self.worker.services_not_available.set.called)
        os.environ["SANDBOX_MODE"] = t

    def test_check_services_mock(self):
        self.worker = EdrDataBridge(config)
        self.worker.check_proxy = self.worker.check_openprocurement_api = self.worker.check_doc_service = MagicMock()
        self.worker.set_wake_up = MagicMock()
        self.worker.set_sleep = MagicMock()
        self.worker.check_services()
        self.assertTrue(self.worker.set_wake_up.called)
        self.worker.check_doc_service = MagicMock(side_effect=RequestError())
        self.worker.check_services()
        self.assertTrue(self.worker.set_sleep.called)


    @patch("gevent.sleep")
    def test_check_log(self, gevent_sleep):
        gevent_sleep = custom_sleep
        self.worker = EdrDataBridge(config)
        self.worker.edrpou_codes_queue = MagicMock(qsize=MagicMock(side_effect=Exception()))
        self.worker.check_services = MagicMock(return_value=True)
        self.worker.run()
        self.assertTrue(self.worker.edrpou_codes_queue.qsize.called)


