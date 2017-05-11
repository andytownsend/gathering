
from auth import gcs_auth as authenticate
import logging
import subprocess


def gcs_upload_file (source_file, bucket_name, content_type, gcs_remote_file):

    storage = authenticate.get_gcs_service()
    bucket = storage.get_bucket(bucket_name)
    blob = bucket.blob(gcs_remote_file)
    print 'remote file: ' + gcs_remote_file
    blob.upload_from_filename(filename=source_file, content_type=content_type)


def gcs_upload_file_list(source_file_list, bucket_name, content_type):
    storage = authenticate.get_gcs_service()
    bucket = storage.get_bucket(bucket_name)

    for file_pair in source_file_list:
        source_file = file_pair[0]
        gcs_remote_file = file_pair[1]
        blob = bucket.blob(gcs_remote_file)

        logging.info("Uploading to: " + gcs_remote_file)
        print "Uploading to: " + gcs_remote_file
        blob.upload_from_filename(filename=source_file, content_type=content_type)

# ('2017/05/08/gb/2017_05_08_gb_tablet_6014_grossing_18_02.json.gz')
# (filename='./gathers/2017/05/08/gb/2017_05_08_gb_phone_6014_grossing_23_37.json.gz')