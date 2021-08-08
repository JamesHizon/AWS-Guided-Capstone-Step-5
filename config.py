# Create Configuration File

# Link for Help: https://blog.finxter.com/creating-reading-updating-a-config-file-with-python/

# NOTE:
# - I need to find a way to import this Python script and create a function s.t.
# I can just call create_config function.

import os
import configparser


# Okay, now let us use above logic to create some function

def create_config(config_file):
    """
    Use function to create config file and then return that same config object that can be

    :config_file: File path object to write to.
    :return: Object by which I can apply the "get" method to retrieve log_file path. --> Is it config so that I can apply "config.get()" method?
    """
    config = configparser.ConfigParser()

    # Add the structure to the file we will create

    config.add_section("mysql")
    config.set("mysql", "user", "root")
    config.set("mysql", "password", "Romans116")
    config.set("mysql", "host", "localhost")
    config.set("mysql", "port", "3306")
    config.set("mysql", "database", "tracker_db")

    # Use for [aws] as a section (note that my account has no access to AWS access and secret access key).
    # config.add_section("aws")
    # config.set("aws", "access_key", "")
    # config.set("secret_access_key", "secret_access_key", "")

    config.add_section("paths")
    config.set("paths", "log_file", "")

    config.add_section("production")
    config.set("production", "processing_date", "2020-08-06")
    config.set("production", "prev_processing_date", "2020-08-05")

    # Write the new structure to the new file
    cfg_file_path = os.getcwd() + "/configfile.ini"

    with open(cfg_file_path, "w") as config_file:
        config.write(config_file)

    return config


