import json
import logging
import my_secrets
import os
import re

from dateutil.parser import parse
from logging import Logger
from sqlalchemy import create_engine, exc, text

# MAIN SQL DB connection constants
DB_HOSTNAME = f"{my_secrets.debian_dbhost}"
DB_NAME = f"{my_secrets.debian_dbname}"
DB_USER = f"{my_secrets.debian_dbuser}"
DB_PW = f"{my_secrets.debian_dbpass}"
# SQL Table names
COMMUNITY_NAMES: str = "communities"
PARCEL_CONSTANTS: str = "parcels"
PARCEL_OWNERS: str = "owners"
PARCEL_SALES: str = "sales"
PARCEL_RENTALS: str = "rentals"

logger: Logger = logging.getLogger(__name__)


def parse_date(date: str) -> str:
    """Takes a date from API result
    Returns formatted str for mysql date field
    """
    try:
        date_parsed = parse(date)

    except TypeError:
        #  Quick Fix. Needs a default date. Rarely occurs mostly on rental co parcels
        date_parsed = parse("1901-01-01")

    return date_parsed


def parse_apn(apn: str) -> str:
    """Takes an unformatted APN value (xxxxxxxx) from API
    Returns a formatted xxx-xx-xxx str
    """
    apn: str = re.sub(r"(\d{3})(\d{2})(\d{3})", r"\1-\2-\3", apn)

    return apn


def parse_ph_nums(num: str) -> str:
    """Takes phone number field data reponse from API
    Returns a formatted (xxx) xxx-xxxx number, empty fields are all 9's
    """
    if num == "~~~~~~~~~~":
        num: str = "9999999999"
        num: str = re.sub(r"(\d{3})(\d{3})(\d{4})", r"(\1) \2-\3", num)

    elif num is None:
        return num

    else:
        num: str = re.sub(r"(\d{3})(\d{3})(\d{4})", r"(\1) \2-\3", num)

    return num


def get_parcel_apns() -> object:
    """Iterates and sorts community name and base info files for insight processing
    Returns an ascending sorted list of tuples
    :return:
    """
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}"
        )
        with engine.connect() as conn, conn.begin():
            results = conn.execute(text("SELECT APN FROM hoa_insights.parcels;"))
            APNs = results.all()
            APNs = [x[0] for x in APNs]

        return APNs, engine

    except exc.DBAPIError as e:
        logger.error(str(e))


def process_json():
    APNS, engine = get_parcel_apns()
    logger.info(
        "EMULATING Accessing Assessor API by using json files (from API) to test UPDATE TRIGGERS"
    )
    test_parcels = os.listdir("./tests/json_update_data/")

    consumed_parcel_data = []

    for parcel in test_parcels:
        parcel_data_file = open(f"./tests/json_update_data/{parcel}", "r")
        parcel_data_json = json.load(parcel_data_file)
        consumed_parcel_data.append(parcel_data_json)

    for parcel_details in consumed_parcel_data:
        if not parcel_details["Owner"]:
            apn: str = parse_apn(parcel_details["TreasurersTransitionUrl"].split("="))
            owner, mail_to, deed_type = 3 * ("",)
            is_rental: bool = parcel_details["IsRental"]
            last_legal_class: str = parcel_details["Valuations"][0][
                "LegalClassificationCode"
            ]
            deed_date = parse("1901-01-01")
            sales_price = 0
            logger.warning(f"No Owner Identified!! {apn}")

        elif parcel_details["Owner"]:
            apn: str = parse_apn(
                parcel_details["TreasurersTransitionUrl"].split("=")[1]
            )
            deed_date: str = parse_date(parcel_details["Owner"]["DeedDate"])
            deed_type: str = parcel_details["Owner"]["DeedType"]

            if not deed_type:
                deed_type = ""
            mail_to: str = parcel_details["Owner"]["FullMailingAddress"]
            mail_to: str = mail_to.replace(",", "")
            owner: str = parcel_details["Owner"]["Ownership"]

            if "'" in owner:
                owner = owner.replace("'", "''")

            is_rental: int = int(parcel_details["IsRental"])
            last_legal_class: str = parcel_details["Valuations"][0][
                "LegalClassificationCode"
            ]
            sale_date: str = parse_date(parcel_details["Owner"]["SaleDate"])
            sale_price: str = parcel_details["Owner"]["SalePrice"]

            if sale_price is None:
                sale_price = 0
    return consumed_parcel_data
