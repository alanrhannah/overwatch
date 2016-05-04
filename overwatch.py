import csv
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

    def str_to_dt(self, date_string):
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
        self.scrapy_metrics = {
            'Av CR (S)': self.calculate_av_crawl_duration(),
            'Longest CR (S)': max(self.gather_crawl_durations()),
            'Shortest CR (S)': min(self.gather_crawl_durations()),
            'Total Duration': self.calculate_total_duration(),
            'Single CR p/h': self.calculate_single_crawls_per_hour(),
            'Max CR p/h': self.calculate_max_crawls_per_hour(),
            'Single CR p/d': self.calculate_single_crawls_per_day(),
            'Max CR p/d': self.calculate_max_crawls_per_day(),
            'Single CR p/7d': self.calculate_single_crawls_per_week(),
            'Max CR p/7d': self.calculate_max_crawls_per_week()
        }
        import ipdb; ipdb.set_trace()
        return self.scrapy_metrics

    def gather_crawl_outliers(self):
        outliers = {'strt': None,
                    'end': None}

        for item in self.response.json()['finished']:
            if not outliers['strt']:
                outliers['strt'] = self.str_to_dt(item['start_time'])
            elif self.str_to_dt(item['start_time']) < outliers['strt']:
                outliers['strt'] = self.str_to_dt(item['start_time'])

            if not outliers['end']:
                outliers['end'] = self.str_to_dt(item['end_time'])
            elif self.str_to_dt(item['end_time']) > outliers['end']:
                outliers['end'] = self.str_to_dt(item['end_time'])
        import ipdb; ipdb.set_trace()
        return outliers

    def calculate_total_duration(self):
        outliers = self.gather_crawl_outliers()
        total_duration = outliers['end'] - outliers['strt']
        return total_duration.total_seconds()

    def gather_crawl_durations(self):
        crawl_durations = []
        response_dictionary = self.response.json()
        for item in response_dictionary['finished']:
            start_dt = self.str_to_dt(item['start_time'])
            end_dt = self.str_to_dt(item['end_time'])
            duration = self.calculate_delta(start_dt, end_dt)
            crawl_durations.append(duration.total_seconds())
        return crawl_durations

    def calculate_av_crawl_duration(self):
        crawl_durations = self.gather_crawl_durations()
        getcontext().prec = 4
        av_crawl_seconds = Decimal(sum(crawl_durations) / len(crawl_durations))
        av_crawl_seconds = round(av_crawl_seconds, 2)

        return float(av_crawl_seconds)

    def calculate_single_crawls_per_hour(self):
        average_crawl_duration = self.calculate_av_crawl_duration()
        single_crawls_per_hour = Decimal(3660) / Decimal(average_crawl_duration)

        return float(single_crawls_per_hour)

    def calculate_max_crawls_per_hour(self):
        single_crawls_per_hour = self.calculate_single_crawls_per_hour()
        max_crawls_per_hour = single_crawls_per_hour * settings.CONC_SPIDERS

        return float(max_crawls_per_hour)

    def calculate_single_crawls_per_day(self):
        single_crawls_per_hour = self.calculate_single_crawls_per_hour()
        single_crawls_per_day = single_crawls_per_hour * 24

        return float(single_crawls_per_day)

    def calculate_max_crawls_per_day(self):
        max_crawls_per_hour = self.calculate_max_crawls_per_hour()
        max_crawls_per_day = max_crawls_per_hour * settings.CONC_SPIDERS

        return float(max_crawls_per_day)

    def calculate_single_crawls_per_week(self):
        single_crawls_per_day = self.calculate_single_crawls_per_day()
        single_crawls_per_week = single_crawls_per_day * 7
        
        return float(single_crawls_per_week)

    def calculate_max_crawls_per_week(self):
        max_crawls_per_day = self.calculate_max_crawls_per_day()
        max_crawls_per_week = max_crawls_per_day * 7
       
        return float(max_crawls_per_week)

    def write_to_csv(self):
        scrapy_metrics = self.gather_scrapy_metrics()
        fieldnames = scrapy_metrics.keys()
        with open(settings.OUTPUT_FILE, 'wb') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            import ipdb; ipdb.set_trace()

if __name__ == '__main__':
    o = Overwatch()
    import ipdb; ipdb.set_trace()
    #foo = o.gather_scrapy_metrics()
