import datetime as dt

from datetime import date


def log_date():
    now: date = dt.date.today()
    todays_date: str = now.strftime("%D").replace("/", "-")

    return todays_date


def sql_date():
    todays_date: date = dt.date.today()
    # todays_date: str = now.strftime("%D").replace("/", "-")

    return todays_date
