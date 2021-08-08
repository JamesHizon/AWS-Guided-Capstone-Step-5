# Python Script to Run Data Ingestion

# NOTE:
# - I assume that want to use the "Tracker" class to track progress of the data ingestion Python script.

# THINK:
# - How ought I create the rest of this Python script?

import sys
import logging
from Tracker import Tracker
from config import create_config


def track_data_ingestion(config):
    """
    This function will be used to track actions of data ingestion.
    The update_job_status() method will be used to store necessary data (job_id, status, updated_time)
    into MySQL Database.

    Q: Exactly how is this tracking changes to the "Data_Ingestion" method again?
    Q: How should I replicate this same logic to the "EOD_Load"?
    :param config: config_file by which we will obtain data based on sections.
    :return:
    """
    tracker = Tracker("Data_Ingestion", config)
    job_id = tracker.assign_job_id()
    connection = tracker.get_db_connection()
    connection
    try:
        # In addition, create methods to assign job_id and get db connection.
        tracker.ingest_all_data()
        tracker.update_job_status("Successful Data Ingestion.", job_id, connection)
    except Exception as e:
        print(e)
        tracker.update_job_status("Failed Data Ingestion.", job_id, connection)
    return


if __name__ == "__main__":
    print("System first input is {}".format(str(sys.argv[0])))
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        print("System 2nd input is {}".format(str(sys.argv[1])))
    else:
        config_file = '../config/config.ini'
        print("Please specify config file.")

    # Tracker Data Ingestion and log info into log file.

    # Create my_config object to get info from config_file
    my_config = create_config(config_file)

    # Obtain log file from config file
    logger = logging.getLogger(__name__)

    log_file = my_config["paths"].get("log_file")

    # Write data to logfile
    logging.basicConfig(
        # TODO: Review components of logging file i.e. format.
        filename=log_file,
        filemode='w',
        format='%(asctime)s %(message)s',
        datefmt='%m%d%Y %I:%M:%S',
        level=logging.DEBUG
    )

    # StreamHandler object creation
    ch = logging.StreamHandler()

    # Set level for logging
    ch.setLevel(logging.INFO)

    # Call addHandler
    logger.addHandler(ch)

    # Call run_reporter_etl(my_config)
    track_data_ingestion(my_config)

    # Enter logging information.
    logger.info("Daily Reporting ETL Job complete!")
