# 45 is a balanced period for cache eviction
# since data is polled "ROUGHLY" every hour.
from loop.settings import MEDIA_ROOT
from os.path import join as path_join

DATA_POLL_PERIOD = 45*60 # seconds
REPORT_GENERATION_RETRY_DELAY = 60 # seconds
REPORTS_DIR = path_join(MEDIA_ROOT, "reports")
