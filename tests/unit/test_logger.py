import glob
import logging
import os

from ezt.util.logger import EztLogger


def test_init_logger(log_folder, log_folder_path):

    logger1 = EztLogger(logs_destination=log_folder)
    logger2 = EztLogger(logs_destination=log_folder_path)

    assert os.path.isdir(logger1.logs_destination)
    assert os.path.isdir(logger2.logs_destination)


def test_log_info(log_folder):

    logger_enabled = EztLogger(logs_destination=log_folder)
    logger_disabled = EztLogger(logs_destination=None)
    log_message = "info log entry"

    logger_enabled.log_info(log_message)
    logger_disabled.log_info(log_message)

    logged = False
    with open(logger_enabled.filename, "r") as f:
        for line in f:
            if log_message in line:
                if "info" in line.lower():
                    logged = True

    assert logged
    assert logger_disabled.filename == None


def test_log_error(log_folder):

    logger_enabled = EztLogger(logs_destination=log_folder)
    logger_disabled = EztLogger(logs_destination=None)
    log_message = "error log entry"

    logger_enabled.log_error(log_message)
    logger_disabled.log_error(log_message)

    logged = False
    with open(logger_enabled.filename, "r") as f:
        for line in f:
            if log_message in line:
                if "error" in line.lower():
                    logged = True

    assert logged
    assert logger_disabled.filename == None
