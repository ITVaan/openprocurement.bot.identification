# -*- coding: utf-8 -*-
import requests


class ProxyClient(object):
    """Base class for making requests to Proxy server"""

    def __init__(self, host, token, timeout=None, port=6547):
        self.session = requests.Session()
        self.token = token
        self.verify_url = '{host}:{port}/verify'.format(host=host, port=port)
        self.details_url = '{host}:{port}/details'.format(host=host, port=port)
        self.headers = {"Authorization": "Basic {token}".format(token=self.token)}
        self.timeout = timeout

    def verify(self, param, code):
        """Send request to Proxy server to verify EDRPOU code"""
        url = '{url}?{param}={code}'.format(url=self.verify_url, param=param, code=code)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response

    def details(self, id):
        """ Send request to Proxy server to get details."""
        url = '{url}/{id}'.format(url=self.details_url, id=id)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response


class DocServiceClient(object):
    """Base class for making requests to Document Service"""

    def __init__(self, host, user, password, port=6555, timeout=None):
        self.session = requests.Session()
        self.url = '{host}:{port}/upload'.format(host=host, port=port)
        self.user = user
        self.password = password
        self.timeout = timeout

    def upload(self, filename, in_file, content_type):
        files = {'file': (filename, in_file, content_type)}
        response = self.session.post(url=self.url, auth=(self.user, self.password), timeout=self.timeout, files=files)

        return response