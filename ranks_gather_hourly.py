

""" Script to gather rankings for ALL lists on the IOS app store
file format: yyyy/mm/dd/gb/yyyy_mm_dd_gb_15_topfreeapplications_hh_mi.json
"""

import urllib
import datetime, time
import shutil, os, sys, random, gzip
import logging
import pytz
import numpy as np
from gcs import gcs_file_handler as gcs
from conf import ranks_gather_hourly_config as params


NRETRY = 10 # Number of time to retry, backoff time is a fudged: base^n
BACKOFF_BASE = 2.5
COUNTRIES = ['gb','us','au', 'br', 'ca', 'dk', 'fr', 'de', 'id', 'ie', 'it', 'my', 'mx', 'nz', 'no', 'pt', 'ru', 'sa', 'sg', 'es', 'se', 'tr', 'ae']
GENRES = ['36','6014']
LIST_TYPES = ['topfreeapplications', 'topfreeipadapplications', 'toppaidapplications', 'toppaidipadapplications', 'topgrossingapplications', 'topgrossingipadapplications']

# List of dictionaries for list types
LIST_TYPES_DICT = (
                     {'list_type_group':'free','list_type':'topfreeapplications','device':'phone'}
                    ,{'list_type_group':'free','list_type':'topfreeipadapplications','device':'tablet'}
                    ,{'list_type_group':'paid','list_type':'toppaidapplications','device':'phone'}
                    ,{'list_type_group':'paid','list_type':'toppaidipadapplications','device':'tablet'}
                    ,{'list_type_group':'grossing','list_type':'topgrossingapplications','device':'phone'}
                    ,{'list_type_group':'grossing','list_type':'topgrossingipadapplications','device':'tablet'}
)


def download_file(country, list_type, genre, file_name, nretry=0):
    url = 'https://itunes.apple.com/' + country + '/rss/' + list_type + '/limit=200/genre=' + genre + '/json'
    try:
        urllib.urlretrieve(url, file_name)
        status = 1
        logging.info("Retrieved: %s", url)
    except Exception, e:
        logging.critical("Failed to retrieve: %s, retry# %s", url, nretry)
        logging.critical(e)
        print e #To get an email notification
        status = 0
    return status


def validate_folder(folder):
    """ Create a new folder if it doesn't already exist
    """
    try:
        os.makedirs(folder)
    except:
        pass


def make_filename(local_dt, country, genre, list_type, fname_root):
    # Date attributes
    yr = str(local_dt.year)
    mth = '{:02d}'.format(local_dt.month)
    dy = '{:02d}'.format(local_dt.day)
    hr = '{:02d}'.format(local_dt.hour)
    mi = '{:02d}'.format(local_dt.minute)

    # Lookup list type attributes based on LIST_TYPES_DICT
    lt_lkp = filter(lambda x: x['list_type'] == list_type, LIST_TYPES_DICT)[0]
    list_type_group = lt_lkp['list_type_group']
    device = lt_lkp['device']

    # Form folder and file names
    folder = "/".join([fname_root, yr, mth, dy, country])
    validate_folder(folder)
    fname = "_".join([yr, mth, dy, country, device, genre, list_type_group, hr, mi + ".json"])
    return "/".join([folder, fname])


def safe_gather(country, list_type, genre, file_name):
    for ntry in range(NRETRY):

        # File download
        logging.info("Download starting for file: " + file_name)
        print "Download starting for file: " + file_name
        status = download_file(country, list_type, genre, file_name, nretry=ntry)
        logging.info("Download completed for file: " + file_name)

        if status:
            return status
        else:
            time.sleep((BACKOFF_BASE ** ntry) + (random.randint(0, 1000) / 1000.))

    logging.critical(">>>> Failed to gather <<<<")
    return 0


def do_gather(c, local_dt, fname_root):
    """ Do the gather for all lists of a country
    """
    for genre in GENRES:
        for lt in LIST_TYPES:
            fname = make_filename(local_dt, c, genre, lt, fname_root)
            safe_gather(c, lt, genre, fname)


def compress_file(input_file, output_file):
    """ Compress a file
    """
    with open(input_file, 'rb') as f_in, gzip.open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def remove_dir(dir_name):
    """ Delete the directory
    """
    try:
        shutil.rmtree(dir_name)
        status = 0
    except Exception, e:
        logging.critical("Failed to remove: %s", dir_name)
        logging.critical(str(e))
        status = 1
    return status


if __name__ == '__main__':

    fname_root = 'gathers'

    # Loop scheduler, running each hour
    while True:
        time.sleep(55)

        utc = pytz.timezone('UTC')
        now = utc.localize(datetime.datetime.now())
        date = str(now.date())
        now_time = str(now.hour)

        if now.minute == 1:

            # Logging set up
            log_folder = "/".join([fname_root, date, now_time])
            validate_folder(log_folder)
            print "log folder: " + log_folder
            log_fname = "/".join([log_folder, "gather.log"])
            logging.basicConfig(stream=sys.stdout, filename=log_fname, level=logging.DEBUG, format='%(asctime)s %(message)s')

            # -------------------------------------------------
            # Loop through countries and gather ranks
            # -------------------------------------------------
            for c in COUNTRIES:
                now = utc.localize(datetime.datetime.now())
                date = str(now.date())
                now_time = str(now.hour)

                local_dates = [now.astimezone(pytz.timezone(tzone)) for tzone in pytz.country_timezones[c]]
                local_times = [str(ldt).replace("-", "").replace(" ", "")[:10] for ldt in local_dates]
                imin = np.argmin(local_times)
                local_dt = local_dates[imin]
                print 'Gathering for: ' + c + ' on date ' + str(local_dt)
                logging.info("Gathering starting for " + c + " for local date " + str(local_dt))
                do_gather(c, local_dt, fname_root)
                logging.info("Gather is done for: " + c)

            logging.info("Gather is done")

            # Upload to Google Cloud Storage
            logging.info("GCS upload starting")
            print "GCS upload starting"

            # -------------------------------------------------
            # File compression
            # -------------------------------------------------
            files_to_zip = []

            for subdir, dir, files in os.walk(fname_root):
                for filename in files:
                    if filename.endswith('.json'):
                        compress_file(os.path.join(subdir, filename), os.path.join(subdir, filename) + '.gz')
                        logging.info("Successfully compressed file: " + filename)

            # -------------------------------------------------
            # Build list of tuples of src and target GCS files
            # -------------------------------------------------
            file_list = []

            for subdir, dir, files in os.walk(fname_root):
                for filename in files:
                    if filename.endswith('.gz'):
                        full_file_name = os.path.join(subdir, filename)
                        file_list.append((full_file_name, 'ios/' + full_file_name.strip(fname_root + '/')))

            # -------------------------------------------------
            # Upload files to GCS
            # -------------------------------------------------
            '''for src_file in file_list:
                print 'GCS upload starting for file: ' + src_file
                gcs.gcs_upload_file(src_file, 'kub_gathers_hourly', src_file.strip(fname_root + '/'))
            '''
            gcs.gcs_upload_file_list(file_list, params.DEFAULT_BUCKET_NAME, params.FILE_UPLOAD_CONTENT_TYPE)

            # Remove local files and dirs
            print "Removing local directory - starting"
            remove_dir(fname_root)
            print "Removing local directory - finished"

        else:
            continue




