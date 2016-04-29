import requests
import settings
import unittest

from unittest import mock
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

if __name__ == '__main__':
    unittest.main()
