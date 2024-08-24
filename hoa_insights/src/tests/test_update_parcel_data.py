import json
import logging
import my_secrets
import os

from dateutil.parser import parse
from logging import Logger
from sqlalchemy import create_engine, exc, text
from utils.parsers import parse_date, parse_apn


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
