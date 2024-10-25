import logging
import my_secrets

from dateutil.parser import parse
from logging import Logger
from sqlalchemy import create_engine, exc, text, TextClause
from utils.parsers import parse_apn, parse_date, parse_ph_nums

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


def update(latest_data):
    """Takes in latest API data and updates the database tables"""
    logger: Logger = logging.getLogger(__name__)
    if latest_data is None:
        logger.warning("No data found")

    try:
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}")

    except (exc.OperationalError, AttributeError) as e:
        logger.error(str(e))

    with engine.connect() as conn, conn.begin():
        delete_rentals: TextClause = f"DELETE FROM {DB_NAME}.{PARCEL_RENTALS};"
        conn.execute(text(delete_rentals))

        for parcel_details in latest_data:
            if not parcel_details["Owner"]:
                apn: str = parse_apn(parcel_details["TreasurersTransitionUrl"].split("="))
                owner, mail_to, deed_type = 3 * ("",)
                is_rental: bool = parcel_details["IsRental"]
                last_legal_class: str = parcel_details["Valuations"][0]["LegalClassificationCode"]
                deed_date: str = parse("1901-01-01")
                sale_price = 0
                logger.warning(f"No Owner Identified!! {apn}")

            elif parcel_details["Owner"]:
                apn: str = parse_apn(parcel_details["TreasurersTransitionUrl"].split("=")[1])
                
                deed_date: str = parse_date(parcel_details["Owner"]["DeedDate"])
                deed_type: str = parcel_details["Owner"]["DeedType"]

                if not deed_type:
                    deed_type: str = ""

                mail_to: str = parcel_details["Owner"]["FullMailingAddress"].replace(",", "")
                owner: str = parcel_details["Owner"]["Ownership"]

                if "'" in owner:
                    owner = owner.replace("'", "''")

                is_rental: int = int(parcel_details["IsRental"])
                last_legal_class: str = parcel_details["Valuations"][0]["LegalClassificationCode"]
                sale_date: str = parse_date(parcel_details["Owner"]["SaleDate"])
                sale_price: str = parcel_details["Owner"]["SalePrice"]

                if sale_price is None:
                    sale_price = 0

            try:
                insert_qry: TextClause = (
                    f"INSERT INTO hoa_insights.{PARCEL_OWNERS} (APN, OWNER, MAIL_ADX, RENTAL, SALE_DATE, SALE_PRICE, DEED_DATE, DEED_TYPE, LEGAL_CODE) "
                    f"VALUES('{apn}', '{owner}', '{mail_to}', '{is_rental}', '{sale_date}', '{sale_price}','{deed_date}', '{deed_type}', '{last_legal_class}') "
                    f"ON DUPLICATE KEY UPDATE  OWNER='{owner}',MAIL_ADX='{mail_to}',RENTAL='{is_rental}', SALE_DATE='{sale_date}', SALE_PRICE='{sale_price}', DEED_DATE='{deed_date}',DEED_TYPE='{deed_type}', LEGAL_CODE='{last_legal_class}';"
                )
                conn.execute(text(insert_qry))

            except (exc.OperationalError, exc.ProgrammingError, UnboundLocalError) as e:
                logger.error(e)

            if is_rental:
                rental_owner_type: str = parcel_details["RentalInformation"]["OwnershipType"]
                rental_owner_name: str = parcel_details["RentalInformation"]["OwnerName"]
                rental_owner_address: str = parcel_details["RentalInformation"]["OwnerAddress"].replace(",", " ")
                rental_owner_phone: str = parse_ph_nums(parcel_details["RentalInformation"]["OwnerPhone"])

                if isinstance(rental_owner_name, str):
                    rental_owner_name: str = rental_owner_name.replace(",", " ")
                else:
                    rental_owner_name: str = parcel_details["RentalInformation"]["OwnerName"]["Name"].replace(",", " ")

                if parcel_details["RentalInformation"]["AgentName"]:
                    rental_contact_name: str = parcel_details["RentalInformation"]["AgentName"].replace(",", "")
                    rental_contact_address: str = parcel_details["RentalInformation"]["AgentAddress"].replace(",", "")
                    rental_contact_phone: str = parse_ph_nums(parcel_details["RentalInformation"]["AgentPhone"])

                elif parcel_details["RentalInformation"]["BusinessContactName"]:
                    rental_contact_name: str = parcel_details["RentalInformation"]["BusinessContactName"].replace(",", "")
                    rental_contact_address: str = parcel_details["RentalInformation"]["BusinessContactAddress"].replace(",", "")
                    rental_contact_phone: str = parse_ph_nums(parcel_details["RentalInformation"]["BusinessContactPhone"])
                else:
                    rental_contact_name: str = rental_owner_name
                    rental_contact_address: str = rental_owner_address
                    rental_contact_phone: str = rental_owner_phone

                try:
                    insert_qry: TextClause = f"""INSERT INTO hoa_insights.{PARCEL_RENTALS} (APN, OWNER, OWNER_TYPE, CONTACT, CONTACT_ADX, CONTACT_PH)
									VALUES('{apn}', '{rental_owner_name}', '{rental_owner_type}', '{rental_contact_name}', '{rental_contact_address}', '{rental_contact_phone}')
									ON DUPLICATE KEY UPDATE OWNER='{rental_owner_name}', OWNER_TYPE='{rental_owner_type}', CONTACT='{rental_contact_name}', CONTACT_ADX='{rental_contact_address}', CONTACT_PH='{rental_contact_phone}';"""
                    conn.execute(text(insert_qry))

                except exc.OperationalError as e:
                    logger.error(e)
