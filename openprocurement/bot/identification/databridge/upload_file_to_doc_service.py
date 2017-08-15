# -*- coding: utf-8 -*-
from munch import munchify
from gevent.queue import Queue
from retrying import retry

import logging.config
import gevent
from datetime import datetime
from gevent import Greenlet, spawn
from gevent.hub import LoopExit

from openprocurement.bot.identification.databridge.utils import journal_context, create_file
from openprocurement.bot.identification.databridge.journal_msg_ids import DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE, \
    DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE, DATABRIDGE_START_UPLOAD
from openprocurement.bot.identification.databridge.constants import file_name, retry_mult

logger = logging.getLogger(__name__)


class UploadFileToDocService(Greenlet):
    """ Upload file with details """

    def __init__(self, upload_to_doc_service_queue, upload_to_tender_queue, process_tracker, doc_service_client,
                 services_not_available, sleep_change_value, delay=15):
        super(UploadFileToDocService, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        self.delay = delay
        self.process_tracker = process_tracker

        # init client
        self.doc_service_client = doc_service_client

        # init queues for workers
        self.upload_to_doc_service_queue = upload_to_doc_service_queue
        self.upload_to_tender_queue = upload_to_tender_queue

        self.sleep_change_value = sleep_change_value
        # retry queues for workers
        self.retry_upload_to_doc_service_queue = Queue(maxsize=500)
        # blockers
        self.services_not_available = services_not_available

    def upload_to_doc_service(self):
        """Get data from upload_to_doc_service_queue; Create file of the Data.file_content data; If upload successful put Data
        object to upload_file_to_tender, otherwise put Data to retry_upload_file_queue."""
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_and_upload(is_retry=False)
            gevent.sleep(0)

    def retry_upload_to_doc_service(self):
        """Get data from retry_upload_to_doc_service_queue; If upload were successful put Data obj to
        upload_to_tender_queue, otherwise put Data obj back to retry_upload_file_queue"""
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_and_upload(is_retry=True)
            gevent.sleep(0)

    def try_peek_and_upload(self, is_retry):
        try:
            tender_data = self.peek_from_queue(is_retry)
        except LoopExit:
            gevent.sleep(0)
        else:
            self.try_upload_to_doc_service(tender_data, is_retry)

    def peek_from_queue(self, is_retry):
        return self.retry_upload_to_doc_service_queue.peek() if is_retry else self.upload_to_doc_service_queue.peek()

    def try_upload_to_doc_service(self, tender_data, is_retry):
        try:
            response = self.update_headers_and_upload(tender_data, is_retry)
        except Exception as e:
            self.remove_bad_data(tender_data, e, is_retry)
        else:
            self.move_to_tender_if_200(response, tender_data, is_retry)

    def update_headers_and_upload(self, tender_data, is_retry):
        if is_retry:
            return self.update_headers_and_upload_retry(tender_data)
        else:
            return self.doc_service_client.upload(file_name, create_file(tender_data.file_content), 'application/yaml',
                                                  headers={'X-Client-Request-ID': tender_data.doc_id()})

    def update_headers_and_upload_retry(self, tender_data):
        self.doc_service_client.headers.update({'X-Client-Request-ID': tender_data.doc_id()})
        return self.client_upload_to_doc_service(tender_data)

    def remove_bad_data(self, tender_data, e, is_retry):
        logger.exception('Exception while uploading file to doc service {} doc_id: {}. Message: {}. {}'.
                         format(tender_data, tender_data.doc_id(), e, "Removed tender data" if is_retry else ""),
                         extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                               tender_data.log_params()))
        if is_retry:
            self.retry_upload_to_doc_service_queue.get()
            self.process_tracker.update_items_and_tender(tender_data.tender_id, tender_data.item_id)
            raise e
        else:
            self.retry_upload_to_doc_service_queue.put(tender_data)
            self.upload_to_doc_service_queue.get()

    def move_to_tender_if_200(self, response, tender_data, is_retry):
        if response.status_code == 200:
            self.move_to_tender_queue(tender_data, response, is_retry)
        else:
            self.move_data_to_retry_or_leave(response, tender_data, is_retry)

    def move_to_tender_queue(self, tender_data, response, is_retry):
        data = tender_data
        data.file_content = dict(response.json(), **{'meta': {'id': tender_data.doc_id()}})
        self.upload_to_tender_queue.put(data)
        if not is_retry:
            self.upload_to_doc_service_queue.get()
        else:
            self.retry_upload_to_doc_service_queue.get()
        logger.info('Successfully uploaded file to doc service {} doc_id: {}'.format(tender_data, tender_data.doc_id()),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE}, tender_data.log_params()))

    def move_data_to_retry_or_leave(self, response, tender_data, is_retry):
        logger.info('Not successful response from document service while uploading {} doc_id: {}. Response {}'.
                    format(tender_data, tender_data.doc_id(), response.status_code),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                          tender_data.log_params()))
        if not is_retry:
            self.retry_upload_to_doc_service_queue.put(tender_data)
            self.upload_to_doc_service_queue.get()

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def client_upload_to_doc_service(self, tender_data):
        """Process upload request for retry queue objects."""
        return self.doc_service_client.upload(file_name, create_file(tender_data.file_content), 'application/yaml',
                                              headers={'X-Client-Request-ID': tender_data.doc_id()})

    def _start_jobs(self):
        return {'upload_to_doc_service': spawn(self.upload_to_doc_service),
                'retry_upload_to_doc_service': spawn(self.retry_upload_to_doc_service)}

    def _run(self):
        logger.info('Start UploadFileToDocService worker',
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_UPLOAD}, {}))
        self.immortal_jobs = self._start_jobs()
        try:
            while not self.exit:
                gevent.sleep(self.delay)
                self.check_and_revive_jobs()
        except Exception as e:
            logger.error(e)
            gevent.killall(self.immortal_jobs.values(), timeout=5)

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead:
                logger.warning("{} worker dead try restart".format(name), extra=journal_context(
                    {"MESSAGE_ID": 'DATABRIDGE_RESTART_{}'.format(name.lower())}, {}))
                self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
                logger.info("{} worker is up".format(name))

    def shutdown(self):
        self.exit = True
        logger.info('Worker UploadFileToDocService complete his job.')
