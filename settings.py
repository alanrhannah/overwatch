import os


OUTPUT_PATH = os.path.join(os.environ['DATA_EXPORT_DIR'],
                          'crawl_times')

if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)
