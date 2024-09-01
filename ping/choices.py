import os

from loop import settings
from loop.settings import MEDIA_ROOT

DATA_ROOT = os.path.join(settings.BASE_DIR, str(__package__), "data")
DATA_SOURCES = {
    "CSV": os.path.join(DATA_ROOT, "data_sources", "csv"),
}
DATA_SOURCE_PARSE_CHUNK_SIZE = 3000 # records per chunk
# 45 is a balanced period for cache eviction
# since data is polled "ROUGHLY" every hour.
DATA_POLL_PERIOD = 45*60 # seconds
DB_IMPORT_RETRY_DELAY = 60 # seconds
REPORT_GENERATION_RETRY_DELAY = 60 # seconds
REPORTS_DIR = os.path.join(MEDIA_ROOT, "reports")
