import argparse
import csv
import datetime
import os
import requests
import settings
import sys

from decimal import Decimal, getcontext

class Overwatch(object):
    """
    Query a scrapyd listjobs end point.

    Generate metrics from the list of finished jobs in the json response 
    and calculate some metrics. 

    Number of spiders that can run concurrently is defined from the settings
    module.

    Writes the results to a csv file on the disk.
    """

    def __init__(self, arguments):
        self.arguments = arguments
        if self.arguments.port:
            self.query_url = '{}:{}/listjobs.json?project={}'.format(
                self.arguments.domain_name[0],
                self.arguments.port[0],
                self.arguments.project_name[0])
        else:
            self.query_url = '{}/listjobs.json?project={}'.format(
                self.arguments.domain_name[0],
                self.arguments.project_name[0])

        self.con_spiders = self.arguments.concurrent_spiders[0]

        today = datetime.datetime.today().strftime('%d-%m-%Y')
        filename = '{}_{}.csv'.format(today, self.arguments.project_name[0])
        self.output_file = os.path.join(settings.OUTPUT_PATH, filename)
        self.response = requests.get(self.query_url)

    def str_to_dt(self, date_string):
        """
        Covert a string to a datetime datetime type.

        :param date_string: a date in format <"yy-mm-dd hh-mm-ss.mmmmmm">
        :return dt:         a datetime.datetime type object
        """
        dt = datetime.datetime.strptime(
                date_string,
                '%Y-%m-%d %H:%M:%S.%f')
        return dt

    def check_response_code(self):
        """
        Check the response from the scrapyd server.
        Returns false if the status code for the response is not 200, i.e. 
        there is a problem with the server.

        :returns status: boolean
        """
        status = True
        if self.response.status_code != 200:
            print('Request failed, response code: {}'.format(
                self.response.status_code))
            status = False

        return status

    def calculate_delta(self, start_time, end_time):
        """
        Calculate the time difference between two times and return a timedetla
        type.

        :param start_time:         a datetime type object
        :param end_time:           a datetime type object
        :returns calculated_delta: a timedelta type object
        """
        calculated_delta = end_time - start_time
        return calculated_delta

    def gather_scrapy_metrics(self):
        """
        Populate a dictionary with scrapyd metrics.

        :returns scrapy_metrics:  a dictionary object
        """
        self.scrapy_metrics = {
            'Av CR (S)': self.calculate_av_crawl_duration(),
            'Longest CR (S)': max(self.gather_crawl_durations()),
            'Shortest CR (S)': min(self.gather_crawl_durations()),
            'Total Duration': self.calculate_total_duration(),
            'Single CR p/h': self.calculate_single_crawls_per_hour(),
            'Max CR p/h': self.calculate_est_total_crawls_per_hour(),
            'Single CR p/d': self.calculate_single_crawls_per_day(),
            'Max CR p/d': self.calculate_est_total_crawls_per_day(),
            'Single CR p/7d': self.calculate_single_crawls_per_week(),
            'Max CR p/7d': self.calculate_est_total_crawls_per_week(),
            'Completed crawls': self.gather_completed_crawl_count()
        }
        return self.scrapy_metrics

    def gather_crawl_outliers(self):
        """
        Loop through a json response dictionary.

        Compare datetime to datetime in outliers dict, update the dict 
        if conditions are met (i.e. the start time is earlier than the 
        start time saved in the outliers dict).

        Populate a dictionary with the values.

        :returns outliers:  a dictionary object
        """
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
        return outliers

    def calculate_total_duration(self):
        """
        Subract the values of two datetime objects.
        Return the total_seconds value of a timedelta object.

        :returns total_duration.total_seconds:  a float
        """
        outliers = self.gather_crawl_outliers()
        total_duration = outliers['end'] - outliers['strt']
        return total_duration.total_seconds()

    def gather_crawl_durations(self):
        """
        Loop through the response dictionary, calculate delta for each
        finished crawl, append timedelta.totalseconds value for each
        to list.

        :return crawl_durations:  a list of floats
        """
        crawl_durations = []
        response_dictionary = self.response.json()
        for item in response_dictionary['finished']:
            start_dt = self.str_to_dt(item['start_time'])
            end_dt = self.str_to_dt(item['end_time'])
            duration = self.calculate_delta(start_dt, end_dt)
            crawl_durations.append(duration.total_seconds())
        
        return crawl_durations

    def gather_completed_crawl_count(self):
        """
        Gather the number of crawls that have been completed at run time.

        :return completed_crawl_count:  integer
        """
        response_dictionary = self.response.json()
        completed_crawl_count = len(response_dictionary['finished'])      
        return completed_crawl_count

    def calculate_av_crawl_duration(self):
        """
        Calculate the mean of crawl durations.

        Round to 2 decimal places and return as a float.

        :return av_crawl_seconds:  a float
        """
        crawl_durations = self.gather_crawl_durations()
        getcontext().prec = 4
        av_crawl_seconds = Decimal(sum(crawl_durations) / len(crawl_durations))
        av_crawl_seconds = round(av_crawl_seconds, 2)

        return float(av_crawl_seconds)

    def calculate_single_crawls_per_hour(self):
        """
        Work out the number of crawls a single spider can do per hour
        from the average.

        Average crawl duration is recorded in seconds, so divide an hour
        in seconds by the crawl duration to return the number of crawls.

        :returns single_crawls_per_hour:  a float
        """
        av_crawl_duration = self.calculate_av_crawl_duration()
        single_crawls_per_hour = Decimal(3600) / Decimal(av_crawl_duration)

        return float(single_crawls_per_hour)

    def calculate_est_total_crawls_per_hour(self):
        """
        Work out the average number of crawls all spiders can do per hour.

        Number of single crawls for a spider multiplied by the number of 
        spiders deployed, as defined in the settings module.

        :returns est_total_crawls_per_hour:  a float
        """
        single_crawls_per_hour = self.calculate_single_crawls_per_hour()
        est_total_crawls_per_hour = single_crawls_per_hour * \
                                        self.arguments.concurrent_spiders[0]

        return float(est_total_crawls_per_hour)

    def calculate_single_crawls_per_day(self):
        """
        Work out the number of crawls a single spider can do per day, based
        on the average number of crawls for a single spider in a hour.

        Average number of single crawls per hour multiplied by 24

        :returns single_crawls_per_day:  a float
        """
        single_crawls_per_hour = self.calculate_single_crawls_per_hour()
        single_crawls_per_day = round((single_crawls_per_hour * 24), 2)

        return float(single_crawls_per_day)

    def calculate_est_total_crawls_per_day(self):
        """
        Work out the number of crawls all spiders can do per day, based
        on the average number of crawls for all spiders in a hour.

        Estimated total crawls for a spider multiplied by 24

        :returns est_total_crawls_per_day:  a float
        """
        est_total_crawls_per_hour = self.calculate_est_total_crawls_per_hour()
        est_total_crawls_per_day = est_total_crawls_per_hour * 24

        return float(est_total_crawls_per_day)

    def calculate_single_crawls_per_week(self):
        """
        Work out the number of crawls a single spider can do per week, based
        on the average number of crawls for a single spider in a day.

        Average number of single crawls per hour multiplied by 7

        :returns single_crawls_per_week:  a float
        """
        single_crawls_per_day = self.calculate_single_crawls_per_day()
        single_crawls_per_week = single_crawls_per_day * 7
        
        return float(single_crawls_per_week)

    def calculate_est_total_crawls_per_week(self):
        """
        Work out the number of crawls all spiders can do per day, based
        on the average number of crawls for all spiders in a day.

        Estimated total crawls for a spider multiplied by 7

        :returns est_total_crawls_per_week:  a float
        """
        est_total_crawls_per_day = self.calculate_est_total_crawls_per_day()
        est_total_crawls_per_week = est_total_crawls_per_day * 7
       
        return float(est_total_crawls_per_week)

    def write_to_csv(self):
        """Create a csv file from a dictionary."""
        scrapy_metrics = self.gather_scrapy_metrics()
        fieldnames = scrapy_metrics.keys()
        
        with open(self.output_file, 'w+') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(scrapy_metrics)


def parse_arguments(arguments):
    """
    Add arguments to command line.
    Parse arguments to argparse namesapce.
    
    :params arguments:  a list of arguments from sys.argv
    :returns parser.parse_args(arguments):  argparse namespace object 
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--project_name',
                        help=('The name of your scrapy project'),
                        type=str,
                        nargs=1,
                        required=True)


    parser.add_argument('-d',
                        '--domain_name',
                        help=('The fully qualified domain of your scrapy '
                              'instance. e.g. https://www.example.com'),
                        type=str,
                        nargs=1,
                        required=True)

    parser.add_argument('-P',
                        '--port',
                        help=('The port number of your scrapy project'),
                        type=str,
                        nargs=1)

    parser.add_argument('-s',
                        '--concurrent_spiders',
                        help=('The number of spiders your scrapyd instance'
                              ' can process concurrently'),
                        type=int,
                        nargs=1)

    return parser.parse_args(arguments)


if __name__ == '__main__':
    arguments = parse_arguments(sys.argv[1:])
    Overwatch(arguments).write_to_csv()
