# -*- coding: utf-8 -*-
from munch import munchify
from gevent.queue import Queue
from retrying import retry

import logging.config
import gevent
from datetime import datetime
from gevent import Greenlet, spawn
from gevent.hub import LoopExit
from restkit import ResourceError

from openprocurement.bot.identification.databridge.utils import journal_context, Data, create_file
from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE, DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE,
    DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_TO_DOC_SERVICE, DATABRIDGE_SUCCESS_UPLOAD_TO_TENDER,
    DATABRIDGE_UNSUCCESS_UPLOAD_TO_TENDER, DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_TO_TENDER, DATABRIDGE_START_UPLOAD,
    DATABRIDGE_422_UPLOAD_TO_TENDER
)

logger = logging.getLogger(__name__)


class UploadFile(Greenlet):
    """ Upload file with details """

    pre_qualification_procurementMethodType = ('aboveThresholdEU', 'competitiveDialogueUA', 'competitiveDialogueEU')
    qualification_procurementMethodType = ('aboveThresholdUA', 'aboveThresholdUA.defense', 'aboveThresholdEU', 'competitiveDialogueUA.stage2', 'competitiveDialogueEU.stage2')

    def __init__(self, client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client, delay=15):
        super(UploadFile, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        self.delay = delay
        self.processing_items = processing_items

        # init clients
        self.client = client
        self.doc_service_client = doc_service_client

        # init queues for workers
        self.upload_to_doc_service_queue = upload_to_doc_service_queue
        self.upload_to_tender_queue = upload_to_tender_queue

        # retry queues for workers
        self.retry_upload_to_doc_service_queue = Queue(maxsize=500)
        self.retry_upload_to_tender_queue = Queue(maxsize=500)

    def upload_to_doc_service(self):
        """Get data from upload_to_doc_service_queue; Create file of the Data.file_content data; If upload successful put Data
        object to upload_file_to_tender, otherwise put Data to retry_upload_file_queue."""
        while not self.exit:
            try:
                tender_data = self.upload_to_doc_service_queue.get()
                document_id = tender_data.file_content.get('meta', {}).get('id')
            except LoopExit:
                gevent.sleep(0)
                continue
            try:
                response = self.doc_service_client.upload('edr_request.yaml', create_file(tender_data.file_content), 'application/yaml',
                                                          headers={'X-Client-Request-ID': document_id})
            except Exception as e:
                logger.warning('Exception while uploading file to doc service {} {} {}. Message: {}. '
                               'Put tender_data to retry queue '.format(tender_data.tender_id, tender_data.item_name,
                                                                        tender_data.item_id, e.message),
                               extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                                      params={"TENDER_ID": tender_data.tender_id,
                                                              "ITEM_ID": tender_data.item_id, "DOCUMENT_ID": document_id}))
                logger.exception(e)
                self.retry_upload_to_doc_service_queue.put(tender_data)
            else:
                if response.status_code == 200:
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, tender_data.edr_ids,
                                dict(response.json(), **{'meta': {'id': document_id}}))
                    self.upload_to_tender_queue.put(data)
                    logger.info('Successfully uploaded file to doc service {} {} {} {}'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE},
                                              params={"TENDER_ID": tender_data.tender_id,
                                                      "DOCUMENT_ID": document_id}))
                else:
                    logger.info('Not successful response from document service while uploading {} {} {} {}. Response {}'.
                                format(tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, response.status_code),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                                    params={"TENDER_ID": tender_data.tender_id,
                                                            "ITEM_ID": tender_data.item_id,
                                                            "DOCUMENT_ID": document_id}))
                    self.retry_upload_to_doc_service_queue.put(tender_data)
            gevent.sleep(0)

    def retry_upload_to_doc_service(self):
        """Get data from retry_upload_to_doc_service_queue; If upload were successful put Data obj to
        upload_to_tender_queue, otherwise put Data obj back to retry_upload_file_queue"""
        while not self.exit:
            try:
                tender_data = self.retry_upload_to_doc_service_queue.get()
                document_id = tender_data.file_content.get('meta', {}).get('id')
            except LoopExit:
                gevent.sleep(0)
                continue
            try:
                # create patch request to award/qualification with document to upload
                self.client.headers.update({'X-Client-Request-ID': document_id})
                response = self.client_upload_to_doc_service(tender_data)
            except Exception as e:
                logger.warning('Exception while uploading file to doc service {} {} {} {}. Message: {}. '
                               'Lost tender_data'.format(tender_data.tender_id, tender_data.item_name,
                                                         tender_data.item_id, document_id, e.message),
                               extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                                      params={"TENDER_ID": tender_data.tender_id,
                                                              "ITEM_ID": tender_data.item_id,
                                                              "DOCUMENT_ID": document_id}))
                logger.exception(e)
                raise e
            else:
                if response.status_code == 200:
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, tender_data.edr_ids,
                                dict(response.json(), **{'meta': {'id': document_id}}))
                    self.upload_to_tender_queue.put(data)
                    logger.info('Successfully uploaded file to doc service {} {} {} {}'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE},
                                              params={"TENDER_ID": tender_data.tender_id,
                                                      "DOCUMENT_ID": document_id}))
                else:
                    logger.info('Not successful response in retry from document service while uploading {} {} {} {}. Response {}'.
                                format(tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, response.status_code),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_TO_DOC_SERVICE},
                                                      params={"TENDER_ID": tender_data.tender_id,
                                                              "ITEM_ID": tender_data.item_id,
                                                              "DOCUMENT_ID": document_id}))
                    self.retry_upload_to_doc_service_queue.put(tender_data)
            gevent.sleep(0)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def client_upload_to_doc_service(self, tender_data):
        """Process upload request for retry queue objects."""
        return self.doc_service_client.upload('edr_request.yaml', create_file(tender_data.file_content),
                                              'application/yaml',
                                              headers={'X-Client-Request-ID': tender_data.file_content.get('meta', {}).get('id')})

    def upload_to_tender(self):
        """Get data from upload_to_tender_queue; Upload get_Url and documentType;
        If upload to tender were unsuccessful put Data object to retry_upload_to_tender_queue, otherwise delete given
        award/qualification from processing_items."""
        while not self.exit:
            try:
                tender_data = self.upload_to_tender_queue.get()
            except LoopExit:
                gevent.sleep(0)
                continue
            document_data = tender_data.file_content.get('data', {})
            document_id = tender_data.file_content.get('meta', {}).get('id')
            document_data["documentType"] = "registerExtract"
            try:
                self.client.headers.update({'X-Client-Request-ID': document_id})
                self.client._create_tender_resource_item(munchify({'data': {'id': tender_data.tender_id}}),
                                                         {'data': document_data},
                                                         '{}/{}/documents'.format(tender_data.item_name,
                                                                                  tender_data.item_id))
            except ResourceError as re:
                if re.status_int == 422:  # WARNING and don't retry
                    logger.warn("Accept 422, skip tender {} {} {} {}.".format(tender_data.tender_id,
                                                                              tender_data.item_name,
                                                                              tender_data.item_id, document_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_422_UPLOAD_TO_TENDER},
                                                      {"TENDER_ID": tender_data.tender_id,
                                                       "DOCUMENT_ID": document_id}))
                    self.update_processing_items(tender_data.tender_id, tender_data.item_id)
                    continue
                else:
                    self.retry_upload_to_tender_queue.put(tender_data)
            except Exception as e:
                logger.info('Exception while uploading file to tender {} {} {} {}. Message: {}'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, e.message),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_TENDER},
                                          params={"TENDER_ID": tender_data.tender_id,
                                                  "DOCUMENT_ID": document_id}))
                self.retry_upload_to_tender_queue.put(tender_data)
            else:
                logger.info('Successfully uploaded file to tender {} {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_TENDER},
                                          params={"TENDER_ID": tender_data.tender_id,
                                                  "DOCUMENT_ID": document_id}))
                # delete current tender after successful upload file (to avoid reloading file)
                self.update_processing_items(tender_data.tender_id, tender_data.item_id)
            gevent.sleep(0)

    def retry_upload_to_tender(self):
        """Get data from retry_upload_to_tender_queue; If upload was unsuccessful put Data obj back to
        retry_upload_to_tender_queue"""
        while not self.exit:
            try:
                tender_data = self.retry_upload_to_tender_queue.get()
            except LoopExit:
                gevent.sleep(0)
                continue
            try:
                document_id = tender_data.file_content.get('meta', {}).get('id')
                self.client.headers.update({'X-Client-Request-ID': document_id})
                self.client_upload_to_tender(tender_data)
            except ResourceError as re:
                if re.status_int == 422:  # WARNING and don't retry
                    logger.warn("Accept 422, skip tender {} {} {} {}.".format(tender_data.tender_id, tender_data.item_name,
                                                                              tender_data.item_id, document_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_422_UPLOAD_TO_TENDER},
                                                      {"TENDER_ID": tender_data.tender_id,
                                                       "DOCUMENT_ID": document_id}))
                    self.update_processing_items(tender_data.tender_id, tender_data.item_id)
                    continue
                else:
                    logger.info('Exception while retry uploading file to tender {} {} {} {}. Message: {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, re.message),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_TO_TENDER},
                                              params={"TENDER_ID": tender_data.tender_id,
                                                      "DOCUMENT_ID": document_id}))
                    logger.exception(re)
                    self.retry_upload_to_tender_queue.put(tender_data)
            except Exception as e:
                logger.info('Exception while retry uploading file to tender {} {} {} {}. Message: {}'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, e.message),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_TO_TENDER},
                                          params={"TENDER_ID": tender_data.tender_id,
                                                  "DOCUMENT_ID": document_id}))
                logger.exception(e)
                self.retry_upload_to_tender_queue.put(tender_data)
            else:
                logger.info('Successfully uploaded file to tender {} {} {} {} in retry'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_TENDER},
                                          params={"TENDER_ID": tender_data.tender_id,
                                                  "DOCUMENT_ID": document_id}))
                # delete current tender after successful upload file (to avoid reloading file)
                self.update_processing_items(tender_data.tender_id, tender_data.item_id)
            gevent.sleep(0)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def client_upload_to_tender(self, tender_data):
        """Process upload to tender request for retry queue objects."""
        document_data = tender_data.file_content.get('data', {})
        document_data["documentType"] = "registerExtract"
        self.client.headers.update({'X-Client-Request-ID': tender_data.file_content.get('meta', {}).get('id')})
        self.client._create_tender_resource_item(munchify({'data': {'id': tender_data.tender_id}}),
                                                 {'data': document_data},
                                                 '{}/{}/documents'.format(tender_data.item_name,
                                                                          tender_data.item_id))

    def update_processing_items(self, tender_id, item_id):
        key = '{}_{}'.format(tender_id, item_id)
        if self.processing_items[key] > 1:
            self.processing_items[key] -= 1
        else:
            del self.processing_items[key]

    def _run(self):
        logger.info('Start UploadFile worker', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_UPLOAD}, {}))
        self.immortal_jobs = {'upload_to_doc_service': spawn(self.upload_to_doc_service),
                              'upload_to_tender': spawn(self.upload_to_tender),
                              'retry_upload_to_doc_service': spawn(self.retry_upload_to_doc_service),
                              'retry_upload_to_tender': spawn(self.retry_upload_to_tender)}

        try:
            while not self.exit:
                gevent.sleep(self.delay)
                for name, job in self.immortal_jobs.items():
                    if job.dead:
                        logger.warning("{} worker dead try restart".format(name), extra=journal_context({"MESSAGE_ID": 'DATABRIDGE_RESTART_{}'.format(name.lower())}, {}))
                        self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
                        logger.info("{} worker is up".format(name))

        except Exception as e:
            logger.error(e)
            gevent.killall(self.immortal_jobs.values(), timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker UploadFile complete his job.')
