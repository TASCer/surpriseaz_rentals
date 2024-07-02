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
            results = conn.execute(
                text(f"SELECT APN FROM {DB_NAME}.{PARCEL_CONSTANTS};")
            )
            APNs = results.all()
            APNs = [x[0] for x in APNs]

        return APNs, engine

    except exc.DBAPIError as e:
        logger.error(str(e))


def process_json():
    APNS, engine = get_parcel_apns()
    logger.info(
        "EMULATING Accessing Assessor API by using json files (from API) to test insight processing"
    )
    test_parcels = os.listdir("./tests/json_seed_data/")

    consumed_parcel_data = []

    for parcel in test_parcels:
        parcel_data_file = open(f"./tests/json_seed_data/{parcel}", "r")
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

        try:
            with engine.connect() as conn, conn.begin():
                insert_qry = f"""INSERT INTO {DB_NAME}.{PARCEL_OWNERS} (APN, OWNER, MAIL_ADX, RENTAL, SALE_DATE, SALE_PRICE, DEED_DATE, DEED_TYPE, LEGAL_CODE) 
				VALUES('{apn}', '{owner}', '{mail_to}', '{is_rental}', '{sale_date}', '{sale_price}','{deed_date}', '{deed_type}', '{last_legal_class}') 
				ON DUPLICATE KEY UPDATE  
					OWNER='{owner}',MAIL_ADX='{mail_to}',RENTAL='{is_rental}', SALE_DATE='{sale_date}', SALE_PRICE='{sale_price}', DEED_DATE='{deed_date}', 
					DEED_TYPE='{deed_type}', LEGAL_CODE='{last_legal_class}';"""

                conn.execute(text(insert_qry))

        except (exc.OperationalError, exc.ProgrammingError, UnboundLocalError) as e:
            logger.error(e)

        if is_rental:
            rental_owner_type: str = parcel_details["RentalInformation"][
                "OwnershipType"
            ]
            rental_owner_name: str = parcel_details["RentalInformation"]["OwnerName"]
            rental_owner_address: str = parcel_details["RentalInformation"][
                "OwnerAddress"
            ]
            rental_owner_address: str = rental_owner_address.replace(",", " ")
            rental_owner_phone: str = parse_ph_nums(
                parcel_details["RentalInformation"]["OwnerPhone"]
            )

            if isinstance(rental_owner_name, str):
                rental_owner_name: str = rental_owner_name.replace(",", " ")
            else:
                rental_owner_name: str = parcel_details["RentalInformation"][
                    "OwnerName"
                ]["Name"]
                rental_owner_name: str = rental_owner_name.replace(",", " ")

            if parcel_details["RentalInformation"]["AgentName"]:
                rental_contact_name: str = parcel_details["RentalInformation"][
                    "AgentName"
                ]
                rental_contact_name: str = rental_contact_name.replace(",", "")
                rental_contact_address: str = parcel_details["RentalInformation"][
                    "AgentAddress"
                ]
                rental_contact_address: str = rental_contact_address.replace(",", "")
                rental_contact_phone: str = parse_ph_nums(
                    parcel_details["RentalInformation"]["AgentPhone"]
                )
            elif parcel_details["RentalInformation"]["BusinessContactName"]:
                rental_contact_name: str = parcel_details["RentalInformation"][
                    "BusinessContactName"
                ]
                rental_contact_name = rental_contact_name.replace(",", "")
                rental_contact_address: str = parcel_details["RentalInformation"][
                    "BusinessContactAddress"
                ]
                rental_contact_address: str = rental_contact_address.replace(",", "")
                rental_contact_phone: str = parse_ph_nums(
                    parcel_details["RentalInformation"]["BusinessContactPhone"]
                )
            else:
                rental_contact_name: str = rental_owner_name
                rental_contact_address: str = rental_owner_address
                rental_contact_phone: str = rental_owner_phone

            try:
                with engine.connect() as conn, conn.begin():
                    insert_qry = f"""INSERT INTO hoa_insights.{PARCEL_RENTALS} (APN, OWNER, OWNER_TYPE, CONTACT, CONTACT_ADX, CONTACT_PH) 
						VALUES('{apn}', '{rental_owner_name}', '{rental_owner_type}', '{rental_contact_name}', '{rental_contact_address}', '{rental_contact_phone}') 
						ON DUPLICATE KEY UPDATE 
							OWNER='{rental_owner_name}', OWNER_TYPE='{rental_owner_type}', CONTACT='{rental_contact_name}', 
							CONTACT_ADX='{rental_contact_address}', CONTACT_PH='{rental_contact_phone}';"""

                    conn.execute(text(insert_qry))
            except exc.OperationalError as e:
                logger.error(e)
