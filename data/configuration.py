import logging

config = {
    "app_queue": "wadi_sms:scheduler",
    "sms_queue": "wadi_sms:sms",
    "event_queue": "wadi_sms:ping",
    "data_url": "https://docs.google.com/spreadsheets/d/1xjPWC3pBZVTDo4bYJAHkr7TyHyDO9YBS7XDbMCgmbIE/export?format=csv",
    "content_url": "https://docs.google.com/spreadsheets/d/144fuYSOgi8md4n2Ezoj9yNMi6AigoXrkHA9rWIF0EDw/export?format=zip",
    "sms_post_url": "http://www.smscountry.com/smscwebservice_bulk.aspx",
    "logFile": "/home/asharma/Comm-Layer/logs/critical.log",
    # "logFile": "./logs/critical.log",
    "logFormat": "[%(name)s: %(levelname)s] %(asctime)s -- %(message)s"
}

file_handler = logging.FileHandler(config['logFile'])
formatter = logging.Formatter(config['logFormat'])
file_handler.setFormatter(formatter)

def createLogger(name):
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    return logger