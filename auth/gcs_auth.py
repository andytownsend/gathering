
from conf import ranks_gather_hourly_config as params
from google.cloud import storage
from google.oauth2 import service_account


def authenticate():
    """ Authenticate with Oauth
    """
    credentials = service_account.Credentials.from_service_account_file(params.KEY_PATH_JSON)
    scoped_credentials = credentials.with_scopes([params.SCOPE_STORAGE, params.SCOPE_QUEUES])
    return scoped_credentials


def get_gcs_service():
    """ Get a service object for the Google API - using Google Cloud
    """
    cred = authenticate()
    print params.PROJECT, cred

    # Instantiate a client
    client = storage.Client(project=params.PROJECT, credentials=cred)

    return client


