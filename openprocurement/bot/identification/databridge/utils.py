# -*- coding: utf-8 -*-
from copy import deepcopy

import yaml
import io

from uuid import uuid4
from collections import namedtuple
from restkit import ResourceError

from openprocurement.bot.identification.databridge.constants import file_name

id_passport_len = 9


class Data(object):
    def __init__(self, tender_id, item_id, code, item_name, file_content):
        self.tender_id = tender_id  # tender ID
        self.item_id = item_id  # qualification or award ID
        self.code = code  # EDRPOU, IPN or passport
        self.item_name = item_name  # "qualifications" or "awards"
        self.file_content = deepcopy(file_content)  # details for file

    def __str__(self):
        return "tender {} {} id: {}".format(self.tender_id, self.item_name[:-1], self.item_id)

    def __eq__(self, other):
        return (self.tender_id == other.tender_id and
                self.item_id == other.item_id and
                self.code == other.code and
                self.item_name == other.item_name and
                self.file_content == other.file_content
                )


def data_string(data):
    return "tender {} {} id: {}".format(data.tender_id, data.item_name[:-1], data.item_id)


def journal_context(record={}, params={}):
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def generate_req_id():
    return b'edr-api-data-bridge-req-' + str(uuid4()).encode('ascii')


def generate_doc_id():
    return uuid4().hex


def validate_param(code):
    return 'id' if code.isdigit() and len(code) != id_passport_len else 'passport'


def create_file(details):
    """ Return temp file with details """
    temporary_file = io.BytesIO()
    temporary_file.name = file_name
    temporary_file.write(yaml.safe_dump(details, allow_unicode=True, default_flow_style=False))
    temporary_file.seek(0)

    return temporary_file


class RetryException(Exception):
    pass


def check_add_suffix(list_ids, document_id, number):
    """Check if EDR API returns list of edr ids with more then 1 element add suffix to document id"""
    len_list_ids = len(list_ids)
    if len_list_ids > 1:
        return '{document_id}.{amount}.{number}'.format(document_id=document_id, amount=len_list_ids, number=number)
    return document_id


def check_412(func):
    def func_wrapper(obj, *args, **kwargs):
        try:
            response = func(obj, *args, **kwargs)
        except ResourceError as re:
            if re.status_int == 412:
                obj.headers['Cookie'] = re.response.headers['Set-Cookie']
                response = func(obj, *args, **kwargs)
            else:
                raise ResourceError(re)
        return response

    return func_wrapper
