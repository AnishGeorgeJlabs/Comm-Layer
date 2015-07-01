# Just some configurations for the system, all at one place

config = {
    # RabbitMQ settings
    "queue": "test",                # test | smsWatcherProduction

    # SMS api
    "data_url": "https://docs.google.com/spreadsheets/d/1xjPWC3pBZVTDo4bYJAHkr7TyHyDO9YBS7XDbMCgmbIE/export?format=csv",
    "content_url": "https://docs.google.com/spreadsheets/d/144fuYSOgi8md4n2Ezoj9yNMi6AigoXrkHA9rWIF0EDw/export?format=zip",
    "sms_post_url": "http://www.smscountry.com/smscwebservice_bulk.aspx"
}