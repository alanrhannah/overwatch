import os

SCRAPYD_SERVER_PROTOCOL = 'http://'
SCRAPYD_SERVER_IP = '192.168.124.30'
SCRAPYD_SERVER_PORT = '6800'
SCRAPYD_LIST_JOBS_ENDPOINT = 'listjobs.json'
SCRAPYD_PROJECT_NAME = 'harvestman'
CONC_SPIDERS = 50
OUTPUT_FILE = os.path.join(os.environ['DATA_EXPORT_DIR'],
                          'crawl_times',
                          'crawl_output.csv')
