""" Parameters for the modeling from GAE pull queue
"""
import os

DEFAULT_LIST_TYPES = 'FPG'

PROJECT = 'reflection-dev'
TASKQUEUE = 'model-hier-scale'
SERVICE_NAME = 'taskqueue'
SERVICE_VERSION = 'v1beta2'
DEFAULT_BUCKET_NAME = 'kub_gathers_hourly'
HEADER_VALUES = {"x-goog-project-id": PROJECT}
FILE_UPLOAD_CONTENT_TYPE = 'application/gzip'


N_PROCESSES = 1

LEASE_TIME = 1800
NUM_TASKS = 1
SLEEP_TIME = 300 # Time to sleep when no tasks to lease are found
RELATIVE_LOG_FILE = 'logs/model_pull_hier_scale.log'
RELATIVE_LOG_CONFIG = 'logs/log_hier_scale.config'
local_path = os.path.dirname(os.path.realpath(__file__)).split('/')
LOG_FILE = '/'.join(local_path + [RELATIVE_LOG_FILE])
LOG_CONFIG = '/'.join(local_path + [RELATIVE_LOG_CONFIG])


## Credentials - JSON
SERVICE_ACCOUNT_EMAIL_JSON = 'new-hourly-gather@reflection-dev.iam.gserviceaccount.com'
KEY_PATH_JSON = '/'.join(local_path + ['../auth/reflection-dev-bb07614cdd75.json'])

SCOPE_QUEUES = 'https://www.googleapis.com/auth/taskqueue'
SCOPE_STORAGE = 'https://www.googleapis.com/auth/devstorage.read_write'

# Trigger URL for Precache
URL_PREFIX = "https://reflection-test.appspot.com"
URL_RETRY_DELAY = 5
