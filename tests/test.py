import os
import requests
import requests_mock
import unittest

from overwatch import settings
from types import DictType


class TestSendRequest(unittest.TestCase):
    
    def setUp(self):
        query_url = '{}{}:{}/{}?project={}'.format(
            settings.SCRAPYD_SERVER_PROTOCOL,
            settings.SCRAPYD_SERVER_IP,
            settings.SCRAPYD_SERVER_PORT,
            settings.SCRAPYD_LIST_JOBS_ENDPOINT,
            settings.SCRAPYD_PROJECT_NAME
            )

        self.response = requests.get(query_url)

    def test_request_returns_response_object(self):
        self.assertTrue(self.response, msg='Response recieved')
        self.assertEqual(self.response.status_code, 200)

    def test_request_returns_json_object(self):
        self.assertTrue(self.response.json())
        self.assertEqual(type(self.response.json()), DictType)

class TestParseResponse(unittest.TestCase):
    
    def setUp(self):
        self.session = requests.Session()
        self.adapter = requests_mock.Adapter()
        self.session.mount('mock', self.adapter)

        self.data_file_path = os.path.join(os.getcwd(), 'test_data')
        self.text_file_name = 'scrapyd_list_jobs_response_text.txt'
        self.json_file_name = 'scrapyd_list_jobs_response_json.json'

        self.scrapyd_list_jobs_response_text_path = os.path.join(
            self.data_file_path,
            self.text_file_name)

        self.scrapyd_list_jobs_response_json_path = os.path.join(
            self.data_file_path,
            self.json_file_name)

        with open(self.scrapyd_list_jobs_response_text_path, 'r') as content:
            self.scrapyd_list_jobs_response_text = content.read()

        self.scrapyd_list_jobs_response_json = open(
            self.scrapyd_list_jobs_response_json_path, 'rb').read()

        self.adapter.register_uri(
            'GET',
            'mock://0.0.0.1:6800/listjobs.json?project=harvestman',
            json=self.scrapyd_list_jobs_response_json,
            status_code=200,
            )

        self.response = self.session.get(
            'mock://0.0.0.1:6800/listjobs.json?project=harvestman')

    def test_request_returns_response_object(self):
        self.assertTrue(self.response, msg='Response recieved')
        self.assertEqual(self.response.status_code, 200)

    def test_request_returns_json_object(self):
        self.assertTrue(self.response.json())
        self.assertEqual(type(self.response.json()), DictType)

if __name__ == '__main__':
    unittest.main()
