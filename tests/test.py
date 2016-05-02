import datetime
import json
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

        self.json_dict = json.loads(self.scrapyd_list_jobs_response_json)

        self.adapter.register_uri(
            'GET',
            'mock://0.0.0.1:6800/listjobs.json?project=harvestman',
            json=self.json_dict,
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

    def test_response_json_keys(self):
        keys = [u'rnning', u'finished', u'stats', u'pending', u'node_name']
        self.assertEqual(self.response.json().keys(), keys)

    def test_response_json_list_lengths(self):
        self.assertEqual(len(self.response.json()['finished']), 100)
        self.assertEqual(len(self.response.json()['rnning']), 0)
        self.assertEqual(len(self.response.json()['pending']), 0)
        self.assertEqual(len(self.response.json()['node_name']), 4)
        self.assertEqual(len(self.response.json()['stats']), 2)

    def test_response_finished_keys(self):
        keys = [u'id', u'start_time', u'spider', u'end_time']
        for row in self.response.json()['finished']:
            self.assertEqual(row.keys(), keys)

    def parse_to_datetime(self, date_string):
        dt = datetime.datetime.strptime(
                date_string,
                '%Y-%m-%d %H:%M:%S.%f')
        return dt

    def test_parse_datetime(self):
        # 2016-04-29 10:28:08.004732
        expected = datetime.datetime(2016, 04, 29, 10, 28, 8, 4732)
        self.assertEqual(
            self.parse_to_datetime(
                self.response.json()['finished'][0]['start_time']), expected)

    def test_subtract_start_end_datetime(self):
        expected = datetime.timedelta(0, 241, 547793)
        self.assertEqual(
            (self.parse_to_datetime(
                self.response.json()['finished'][0]['end_time']
                ) -
            self.parse_to_datetime(
                self.response.json()['finished'][0]['start_time']
                ), expected))

if __name__ == '__main__':
    unittest.main()
