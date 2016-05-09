import csv
import datetime
import json
import os
import requests
import requests_mock
import tempfile
import unittest

from decimal import Decimal, getcontext
from overwatch import settings, Overwatch
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

class TestCalculateTimeDifferences(unittest.TestCase):

    def create_file_path(self, file_name):
        path = os.path.join(self.data_file_path, file_name)

        return path 

    def setUp(self):
        self.data_file_path = os.path.join(os.getcwd(), 'test_data')
        self.json_file_name = 'scrapyd_list_jobs_response_json.json'
        self.json_outliers_file_name = 'scrapyd_list_jobs_outliers_json.json'

        self.scrapyd_list_jobs_response_json_path = self.create_file_path(
            self.json_file_name)

        self.scrapyd_outliers_json_path = self.create_file_path(
            self.json_outliers_file_name)

        self.scrapyd_list_jobs_response_json = open(
            self.scrapyd_list_jobs_response_json_path, 'rb').read()

        self.scrapyd_outliers_json = open(
            self.scrapyd_outliers_json_path, 'rb').read()

        self.response_json_dict = json.loads(
            self.scrapyd_list_jobs_response_json)

        outliers_json_dict = json.loads(
            self.scrapyd_outliers_json)

        self.calculated_delta = (self.str_to_dt(
                self.response_json_dict['finished'][0]['end_time']
                ) - self.str_to_dt(
                self.response_json_dict['finished'][0]['start_time']
                ))

        self.earliest_datetime = None
        self.latest_datetime = None

        for item in outliers_json_dict['finished']:
            if self.earliest_datetime is None:
                self.earliest_datetime = self.str_to_dt(item['start_time'])
            elif self.str_to_dt(item['start_time']) < self.earliest_datetime:
                self.earliest_datetime = self.str_to_dt(item['start_time'])

            if self.latest_datetime is None:
                self.latest_datetime = self.str_to_dt(item['end_time'])
            elif self.str_to_dt(item['end_time']) > self.latest_datetime:
                self.latest_datetime = self.str_to_dt(item['end_time'])

    def str_to_dt(self, date_string):
        dt = datetime.datetime.strptime(
                date_string,
                '%Y-%m-%d %H:%M:%S.%f')
        return dt

    def test_parse_datetime(self):
        # 2016-04-29 10:28:08.004732
        expected = datetime.datetime(2016, 04, 29, 10, 28, 8, 4732)
        self.assertEqual(
            self.str_to_dt(
                self.response_json_dict['finished'][0]['start_time']),
            expected)

    def test_subtract_start_end_datetime(self):
        expected = datetime.timedelta(0, 241, 547793)
        self.assertEqual(self.calculated_delta, expected)

    def test_convert_subtraction_to_minutes(self):
        expected = 4.026
        getcontext.prec = 4
        self.calculated_mins = round(
            Decimal(
                self.calculated_delta.total_seconds()) / Decimal(60),
            3)
        self.assertEqual(self.calculated_mins, expected)

    def test_collect_outliers(self):
        self.assertEqual(self.earliest_datetime, 
                         datetime.datetime(2016, 4, 29, 10, 28, 8, 4732))

        self.assertEqual(self.latest_datetime,
                         datetime.datetime(2016, 4, 29, 10, 33, 51, 420786))

    def test_calculate_total_crawl_duration(self):
        duration = self.latest_datetime - self.earliest_datetime
        self.assertEqual(duration.total_seconds(), 343.416054)
        pass

class TestInit(unittest.TestCase):

    def setUp(self):
        self.query_url = Overwatch().query_url

    def test_init(self):
        expected = 'http://192.168.124.30:6800/listjobs.json?project=harvestman'
        self.assertEqual(self.query_url, expected)

class TestStrToDateTime(unittest.TestCase):

    def setUp(self):
        self.str_to_dt = Overwatch().str_to_dt
        pass

    def test_str_to_dt(self):
        date_string = '2016-04-11 20:20:20.111111'
        expected = datetime.datetime(2016, 4, 11, 20, 20, 20, 111111)
        self.assertEqual(self.str_to_dt(date_string), expected)

class TestCheckResponseCode(unittest.TestCase):

    def setUp(self):
        self.session = requests.Session()
        self.adapter = requests_mock.Adapter()
        self.session.mount('mock', self.adapter)
        self.adapter.register_uri(
            'GET',
            'mock://0.0.0.1:6800/ok',
            status_code=200,
            )
        self.adapter.register_uri(
            'GET',
            'mock://0.0.0.1:6800/fail',
            status_code=404,
            )
        self.overwatch = Overwatch()

    def test_check_response_code_true(self):
        self.overwatch.response = self.session.get('mock://0.0.0.1:6800/ok')
        self.assertEqual(self.overwatch.check_response_code(), True)

    def test_check_response_code_false(self):
        self.overwatch.response = self.session.get('mock://0.0.0.1:6800/fail')
        self.assertEqual(self.overwatch.check_response_code(), False)

class TestCalculateDelta(unittest.TestCase):

    def setUp(self):
        self.start_date = datetime.datetime(2016, 1, 1, 0, 0, 0, 0)
        self.end_date = datetime.datetime(2016, 1, 1, 1, 0, 0, 0)
        self.calculate_delta = Overwatch().calculate_delta

    def test_calculate_delta(self):
        expected = datetime.timedelta(0, 3600)
        calculated_delta = self.calculate_delta(self.start_date, self.end_date)
        self.assertEqual(calculated_delta, expected)

class TestOverwatch(unittest.TestCase):

    def create_file_path(self, file_name):
        path = os.path.join(self.data_file_path, file_name)
        return path 

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='temp_test_dir')

        self.data_file_path = os.path.join(os.getcwd(), 'test_data')
        self.overwatch = Overwatch()
        self.json_outliers_file_name = 'scrapyd_list_jobs_outliers_json.json'

        self.scrapyd_outliers_json_path = self.create_file_path(
            self.json_outliers_file_name)

        self.scrapyd_outliers_json = open(self.scrapyd_outliers_json_path,
                                          'rb').read()

        outliers_json_dict = json.loads(self.scrapyd_outliers_json)

        self.session = requests.Session()
        self.adapter = requests_mock.Adapter()
        self.session.mount('mock', self.adapter)

        self.adapter.register_uri(
            'GET',
            'mock://0.0.0.1:6800/listjobs.json?project=harvestman',
            json=outliers_json_dict,
            status_code=200,
            )

        self.overwatch.response = self.session.get(
            'mock://0.0.0.1:6800/listjobs.json?project=harvestman')

    def test_gather_crawl_outliers(self):
        expected = {'strt': datetime.datetime(2016,
                                              4,
                                              29,
                                              10,
                                              28,
                                              8,
                                              4732),
                    'end': datetime.datetime(2016,
                                             4,
                                             29,
                                             10,
                                             33,
                                             51,
                                             420786)
                    }
        self.assertEqual(self.overwatch.gather_crawl_outliers(), expected)

    def test_calculate_total_duration(self):
        expected = 343.416054
        self.assertEqual(self.overwatch.calculate_total_duration(), expected)

    def test_gather_crawl_durations(self):
        expected = [241.547793, 233.489018, 258.652448]
        self.assertEqual(self.overwatch.gather_crawl_durations(), expected)

    def test_calculate_av_crawl_duration(self):
        expected = 244.56
        self.assertEqual(self.overwatch.calculate_av_crawl_duration(),
                         expected)

    def test_calculate_single_crawls_per_hour(self):
        expected = 14.72
        self.assertEqual(self.overwatch.calculate_single_crawls_per_hour(),
                         expected)

    def test_calculate_est_total_crawls_per_hour(self):
        expected = 736
        self.assertEqual(self.overwatch.calculate_est_total_crawls_per_hour(),
                         expected)

    def test_calculate_single_crawls_per_day(self):
        expected = 353.28
        self.assertEqual(self.overwatch.calculate_single_crawls_per_day(),
                         expected)

    def test_calculate_est_total_crawls_per_day(self):
        expected = 17664
        self.assertEqual(self.overwatch.calculate_est_total_crawls_per_day(),
                         expected)  

    def test_calculate_single_crawls_per_week(self):
        expected = 2472.96
        self.assertEqual(self.overwatch.calculate_single_crawls_per_week(),
                         expected)

    def test_calculate_est_total_crawls_per_week(self):
        expected = 123648
        self.assertEqual(self.overwatch.calculate_est_total_crawls_per_week(),
                         expected)  

    def test_gather_scrapy_metrics(self):
        expected = { 
            'Av CR (S)': 244.56, 
            'Longest CR (S)': 258.652448, 
            'Shortest CR (S)': 233.489018, 
            'Total Duration': 343.416054, 
            'Single CR p/h': 14.72, 
            'Max CR p/h': 736, 
            'Single CR p/d': 353.28, 
            'Max CR p/d': 17664, 
            'Single CR p/7d': 2472.96, 
            'Max CR p/7d': 123648 
        } 
        self.assertEqual(self.overwatch.gather_scrapy_metrics(), expected)  

    def test_write_to_csv(self):
        settings.OUTPUT_FILE = os.path.join(self.temp_dir,
                                            settings.filename)
        os.mknod(settings.OUTPUT_FILE)
        self.overwatch.write_to_csv()

        with open(settings.OUTPUT_FILE, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            csv_data = [row for row in reader]
            self.assertEqual(csv_data[0]['Total Duration'], '343.416054')


if __name__ == '__main__':
    unittest.main()
