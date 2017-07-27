# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
from gevent.hub import LoopExit
from gevent.queue import Queue
from openprocurement.bot.identification.databridge.constants import author
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.utils import Data, ProcessTracker, item_key
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_request_id, ResponseMock
from openprocurement.bot.identification.databridge.bridge import TendersClientSync
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import Unauthorized, ResourceError, RequestFailed
from gevent.pywsgi import WSGIServer
from bottle import Bottle, response
from simplejson import dumps

SERVER_RESPONSE_FLAG = 0
SPORE_COOKIES = ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                 "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                 "96a11693fa8bd38623e4daee121f60b4301aef012c")
COOKIES_412 = ("b7afc9b1fc79e640f2487ba48243ca071c07a823d27"
               "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
               "96a11693fa8bd38623e4daee121f60b4301aef012c")


def setup_routing(app, func, path='/api/2.3/spore', method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", SPORE_COOKIES)
    return response


def response_412():
    response.status = 412
    response.set_cookie("SERVER_ID", COOKIES_412)
    return response


def response_get_tender():
    response.status = 200
    response.headers['X-Request-ID'] = '125'
    return dumps({'prev_page': {'offset': '123'},
                  'next_page': {'offset': '1234'},
                  'data': {'status': "active.pre-qualification",
                           'id': '123',
                           'procurementMethodType': 'aboveThresholdEU',
                           'awards': [{'id': '124',
                                       'bid_id': '111',
                                       'status': 'pending',
                                       'suppliers': [{'identifier': {
                                                      'scheme': 'UA-EDR',
                                                      'id': '14360570'}}]}]}})


def generate_response():
    global SERVER_RESPONSE_FLAG
    if SERVER_RESPONSE_FLAG == 0:
        SERVER_RESPONSE_FLAG = 1
        return response_412()
    return response_get_tender()


class TestFilterWorker(unittest.TestCase):

    def setUp(self):
        self.filtered_tender_ids_queue = Queue(10)
        self.edrpou_codes_queue = Queue(10)
        self.process_tracker = ProcessTracker()
        self.request_id = generate_request_id()
        self.tender_id = uuid.uuid4().hex
        self.award_id = uuid.uuid4().hex
        self.bid_id = uuid.uuid4().hex

    def check_data_objects(self, obj, example):
        """Checks that two data objects are equal, 
                  that Data.file_content.meta.id is not none and
                  that Data.file_content.meta.author exists and is equal to IdentificationBot
         """
        self.assertEqual(obj.tender_id, example.tender_id)
        self.assertEqual(obj.item_id, example.item_id)
        self.assertEqual(obj.code, example.code)
        self.assertEqual(obj.item_name, example.item_name)
        self.assertIsNotNone(obj.file_content['meta']['id'])
        self.assertEqual(obj.file_content['meta']['author'], author)
        self.assertEqual(obj.file_content['meta']['sourceRequests'], example.file_content['meta']['sourceRequests'])

    def test_init(self):
        worker = FilterTenders.spawn(None, None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.tenders_sync_client, None)
        self.assertEqual(worker.filtered_tender_ids_queue, None)
        self.assertEqual(worker.edrpou_codes_queue, None)
        self.assertEqual(worker.process_tracker, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_worker_qualification(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        bid_ids = [uuid.uuid4().hex for i in range(5)]
        qualification_ids = [uuid.uuid4().hex for i in range(5)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': self.request_id},
                                                    munchify(
                                                        {'prev_page': {'offset': '123'},
                                                         'next_page': {'offset': '1234'},
                                                         'data': {'status': "active.pre-qualification",
                                                               'id': self.tender_id,
                                                               'procurementMethodType': 'aboveThresholdEU',
                                                               'bids': [{'id': bid_ids[0],
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '14360570'}
                                                                         }]},
                                                                        {'id': bid_ids[1],
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '0013823'}
                                                                         }]},
                                                                        {'id': bid_ids[2],
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '23494714'}
                                                                         }]},
                                                                        {'id': bid_ids[3],
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '23494714'}
                                                                         }]},
                                                                        {'id': bid_ids[4],
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-ED',
                                                                             'id': '23494714'}
                                                                         }]},
                                                                        ],
                                                               'qualifications': [{'status': 'pending',
                                                                                   'id': qualification_ids[0],
                                                                                   'bidID': bid_ids[0]},
                                                                                  {'status': 'pending',
                                                                                   'id': qualification_ids[1],
                                                                                   'bidID': bid_ids[1]},
                                                                                  {'status': 'pending',
                                                                                   'id': qualification_ids[2],
                                                                                   'bidID': bid_ids[2]},
                                                                                  {'status': 'unsuccessful',
                                                                                   'id': qualification_ids[3],
                                                                                   'bidID': bid_ids[3]},
                                                                                  {'status': 'pending',
                                                                                   'id': qualification_ids[4],
                                                                                   'bidID': bid_ids[4]},
                                                                                  ]}}))
        first_data = Data(self.tender_id, qualification_ids[0], '14360570', 'qualifications', {'meta': {'sourceRequests': [self.request_id]}})
        second_data = Data(self.tender_id, qualification_ids[1], '0013823', 'qualifications', {'meta': {'sourceRequests': [self.request_id]}})
        third_data = Data(self.tender_id, qualification_ids[2], '23494714', 'qualifications', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())

        for data in [first_data, second_data, third_data]:
            self.check_data_objects(self.edrpou_codes_queue.get(), data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, qualification_ids[0]),
                               item_key(self.tender_id, qualification_ids[1]),
                               item_key(self.tender_id, qualification_ids[2])])

    @patch('gevent.sleep')
    def test_worker_award(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        bid_ids = [uuid.uuid4().hex for i in range(5)]
        award_ids = [uuid.uuid4().hex for i in range(5)]
        client = MagicMock()
        client.request.side_effect = [ResponseMock({'X-Request-ID': self.request_id},
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': self.tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'awards': [{'id': award_ids[0],
                                           'bid_id': bid_ids[0],
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '14360570'}
                                         }]},
                                        {'id': award_ids[1],
                                         'bid_id': bid_ids[1],
                                         'status': 'pending',
                                         'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '0013823'}
                                         }]},
                                        {'id': award_ids[2],
                                         'bid_id': bid_ids[2],
                                         'status': 'pending',
                                         'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '23494714'}
                                         }]},
                                        {'id': award_ids[3],
                                         'bid_id': bid_ids[3],
                                         'status': 'unsuccessful',
                                         'suppliers': [{'identifier': {
                                            'scheme': 'UA-EDR',
                                            'id': '23494714'}
                                         }]},
                                          {'id': award_ids[4],
                                           'bid_id': bid_ids[4],
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                               'scheme': 'UA-ED',
                                               'id': '23494714'}
                                           }]},
                                        ]
                               }}))]

        first_data = Data(self.tender_id, award_ids[0], '14360570', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        second_data = Data(self.tender_id, award_ids[1], '0013823', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        third_data = Data(self.tender_id, award_ids[2], '23494714', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock(), 2, 1)
        for edrpou in [first_data, second_data, third_data]:
            self.check_data_objects(self.edrpou_codes_queue.get(), edrpou)

        worker.shutdown()
        del worker

        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, award_ids[0]), item_key(self.tender_id, award_ids[1]),
                               item_key(self.tender_id, award_ids[2])])

    @patch('gevent.sleep')
    def test_get_tender_exception(self, gevent_sleep):
        """ We must not lose tender after restart filter worker """
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        client = MagicMock()
        client.request.side_effect = [Exception(),
                                      ResponseMock({'X-Request-ID': self.request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': self.award_id,
                                                                                  'bid_id': self.bid_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': '14360570'}
                                                                                  }]}
                                                                                 ]
                                                                      }}))]
        data = Data(self.tender_id, self.award_id, '14360570', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock(), 2, 1)
        self.check_data_objects(self.edrpou_codes_queue.get(), data)
        worker.shutdown()
        self.assertEqual(worker.sleep_change_value, 0)
        del worker
        gevent_sleep.assert_called_with_once(1)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_get_tender_429(self, gevent_sleep):
        """ We must not lose tender after restart filter worker """
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        client = MagicMock()
        client.request.side_effect = [
                                      ResourceError(http_code=429),
                                      ResponseMock({'X-Request-ID': self.request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': self.award_id,
                                                                                  'status': 'pending',
                                                                                  'bid_id': self.bid_id,
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': '14360570'}
                                                                                  }]}
                                                                                 ]
                                                                      }}))]
        data = Data(self.tender_id, self.award_id, '14360570', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock(), 2, 1)
        self.check_data_objects(self.edrpou_codes_queue.get(), data)
        worker.shutdown()
        self.assertEqual(worker.sleep_change_value, 1)
        del worker
        gevent_sleep.assert_called_with_once(1)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_worker_restart(self, gevent_sleep):
        """ Process tender after catch Unauthorized exception """
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        award_ids = [uuid.uuid4().hex for i in range(2)]
        bid_ids = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.side_effect = [
            Unauthorized(http_code=403),
            Unauthorized(http_code=403),
            Unauthorized(http_code=403),
            ResponseMock({'X-Request-ID': self.request_id},
                    munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': self.tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'awards': [{'id': award_ids[0],
                                           'bid_id': bid_ids[0],
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                               'scheme': 'UA-EDR',
                                               'id': '14360570'}
                                           }]},
                                          {'id': award_ids[1],
                                           'bid_id': bid_ids[1],
                                           'status': 'unsuccessful',
                                           'suppliers': [{'identifier': {
                                               'scheme': 'UA-EDR',
                                               'id': '23494714'}
                                           }]},
                                          ]
                               }}))
        ]
        data = Data(self.tender_id, award_ids[0], '14360570', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())

        self.check_data_objects(self.edrpou_codes_queue.get(), data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, award_ids[0])])

    @patch('gevent.sleep')
    def test_worker_dead(self, gevent_sleep):
        """ Test that worker will process tender after exception  """
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        self.filtered_tender_ids_queue.put(self.tender_id)
        bid_ids = [uuid.uuid4().hex for i in range(2)]
        award_ids = [uuid.uuid4().hex for i in range(2)]
        request_ids = [generate_request_id() for i in range(2)]
        client = MagicMock()
        client.request.side_effect = [
            ResponseMock({'X-Request-ID': request_ids[0]},
                munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': self.tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': award_ids[0],
                          'bid_id': bid_ids[0],
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': '14360570'}}]
                          }]}})),
            ResponseMock({'X-Request-ID': request_ids[1]},
                munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': self.tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': award_ids[1],
                          'bid_id': bid_ids[1],
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': '14360570'}
                               }]}]}}))]
        first_data = Data(self.tender_id, award_ids[0], '14360570', 'awards', {'meta': {'sourceRequests': [request_ids[0]]}})
        second_data = Data(self.tender_id, award_ids[1], '14360570', 'awards', {'meta': {'sourceRequests': [request_ids[1]]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())
        self.check_data_objects(self.edrpou_codes_queue.get(), first_data)
        worker.job.kill(timeout=1)
        self.check_data_objects(self.edrpou_codes_queue.get(), second_data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, award_ids[0]),
                                                        item_key(self.tender_id, award_ids[1])])

    @patch('gevent.sleep')
    def test_filtered_tender_ids_queue_loop_exit(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = MagicMock()
        filtered_tender_ids_queue.peek.side_effect = [LoopExit(), self.tender_id]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': self.request_id},
            munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': self.tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': self.award_id,
                          'bid_id': self.bid_id,
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': '14360570'}}]
                          }]}}))
        first_data = Data(self.tender_id, self.award_id, '14360570', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())
        self.check_data_objects(self.edrpou_codes_queue.get(), first_data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])

    @patch('gevent.sleep')
    def test_worker_award_with_cancelled_lot(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        award_ids = [uuid.uuid4().hex for i in range(2)]
        bid_ids = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': self.request_id},
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': self.tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'lots': [{'status': 'cancelled',
                                         'id': '123456789'},
                                        {'status': 'active',
                                         'id': '12345678'}
                                        ],
                               'awards': [{'id': award_ids[0],
                                           'bid_id': bid_ids[0],
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '14360570'}
                                         }],
                                           'lotID': '123456789'},
                                        {'id': award_ids[1],
                                         'bid_id': bid_ids[1],
                                         'status': 'pending',
                                         'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '0013823'}
                                         }],
                                         'lotID': '12345678'}
                                        ]
                               }}))

        data = Data(self.tender_id, award_ids[1], '0013823', 'awards', {'meta': {'sourceRequests': [self.request_id]}})
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())

        for edrpou in [data]:
            self.check_data_objects(self.edrpou_codes_queue.get(), edrpou)

        worker.shutdown()
        del worker

        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, award_ids[1])])

    @patch('gevent.sleep')
    def test_qualification_not_valid_identifier_id(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        first_bid_id, second_bid_id = [uuid.uuid4().hex for i in range(2)]
        first_qualification_id, second_qualification_id = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': self.request_id},
                                                    munchify(
                                                        {'prev_page': {'offset': '123'},
                                                         'next_page': {'offset': '1234'},
                                                         'data': {'status': "active.pre-qualification",
                                                               'id': self.tender_id,
                                                               'procurementMethodType': 'aboveThresholdEU',
                                                               'bids': [{'id': first_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': 'test@test.com'}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': ''}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': 'абв'}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': u'абв'}
                                                                         }]}
                                                                        ],
                                                               'qualifications': [{'status': 'pending',
                                                                                   'id': first_qualification_id,
                                                                                   'bidID': first_bid_id},
                                                                                  {'status': 'pending',
                                                                                   'id': second_qualification_id,
                                                                                   'bidID': second_bid_id}]}}))
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())

        sleep(1)
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items, {})

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_award_not_valid_identifier_id(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        first_award_id, second_award_id = [uuid.uuid4().hex for i in range(2)]
        bid_ids = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': self.request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': first_award_id,
                                                                                  'bid_id': bid_ids[0],
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': ''}
                                                                                  }]},
                                                                                 {'id': first_award_id,
                                                                                  'bid_id': bid_ids[0],
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': u'абв'}
                                                                                  }]},
                                                                                 {'id': first_award_id,
                                                                                  'bid_id': bid_ids[1],
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': 'абв'}
                                                                                  }]},
                                                                                 {'id': second_award_id,
                                                                                  'bid_id': bid_ids[1],
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': 'test@test.com'}
                                                                                  }]}]
                                                                      }}))
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())

        sleep(1)

        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items, {})

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_412(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put('123')
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        setup_routing(api_server_bottle, generate_response, path='/api/2.3/tenders/123')
        api_server.start()
        client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        self.assertEqual(client.headers['Cookie'], 'SERVER_ID={}'.format(SPORE_COOKIES))  # check that response_spore set cookies
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())
        data = Data('123', '124', '14360570', 'awards', {'meta': {'sourceRequests': ['125']}})

        for i in [data]:
            self.check_data_objects(self.edrpou_codes_queue.get(), i)
        self.assertEqual(client.headers['Cookie'], 'SERVER_ID={}'.format(COOKIES_412))  # check that response_412 change cookies
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), ['123_124'])

        worker.shutdown()
        del worker
        api_server.stop()

    @patch('gevent.sleep')
    def test_request_failed(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        bid_ids = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.side_effect = [RequestFailed(http_code=401, msg=RequestFailed()),
            ResponseMock({'X-Request-ID': self.request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': self.award_id,
                                                                                  'bid_id': bid_ids[0],
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': ''}
                                                                                  }]}]
                                                                      }}))]
        worker = FilterTenders.spawn(client, self.filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker, MagicMock())

        sleep(1)

        self.assertEqual(client.request.call_count, 2)
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items, {})

        worker.shutdown()
        del worker
