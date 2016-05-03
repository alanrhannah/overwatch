import datetime
import json
import os
import requests
import settings
import unittest

from decimal import Decimal, getcontext

class Overwatch(object):

    def __init__(self):
        query_url = '{}{}:{}/{}?project={}'.format(
            settings.SCRAPYD_SERVER_PROTOCOL,
            settings.SCRAPYD_SERVER_IP,
            settings.SCRAPYD_SERVER_PORT,
            settings.SCRAPYD_LIST_JOBS_ENDPOINT,
            settings.SCRAPYD_PROJECT_NAME
            )

        self.response = requests.get(query_url)

    def parse_string_to_datetime(self, date_string):
        dt = datetime.datetime.strptime(
                date_string,
                '%Y-%m-%d %H:%M:%S.%f')
        return dt

    def check_response_code(self):
        status = True
        if self.response.status_code != 200:
            print('Request failed, response code: {}'.format(
                self.response.status_code))
            status = False

        return status

    def calculate_delta(self, start_time, end_time):
        calculated_delta = end_time - start_time
        return calculated_delta

    def gather_scrapy_metrics(self):
        self.scrapy_metrics = {}
        self.scrapy_metrics['Average Crawl Time (S)'] = self.caclulate_av_crawl()
        self.scrapy_metrics['Longest Crawl Time (S)'] = self.longest_crawl()
        self.scrapy_metrics['Shortest Crawl Time'] = self.shortest_crawl()
        self.scrapy_metrics['Total Duration'] = self.caclulate_total_duration()
        return self.scrapy_metrics

    def gather_crawl_durations(self):
        crawl_durations = []
        response_dictionary = self.response.json()
        for item in response_dictionary['finished']:
            start_dt = self.parse_string_to_datetime(item['start_time'])
            end_dt = self.parse_string_to_datetime(item['end_time'])
            duration = self.calculate_delta(start_dt, end_dt)
            crawl_durations.append(duration.total_seconds())
        return crawl_durations

    def calculate_av_crawl(self):
        crawl_durations = self.gather_crawl_durations()
        getcontext().prec = 4
        av_crawl_seconds = Decimal(sum(crawl_durations) / len(crawl_durations))
        av_crawl_seconds = round(av_crawl_seconds, 2)
        return av_crawl_seconds

if __name__ == '__main__':
    o = Overwatch()
    import ipdb; ipdb.set_trace()

