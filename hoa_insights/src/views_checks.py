import datetime as dt
import logging
import my_secrets

from logging import Logger
from sqlalchemy import create_engine, exc, text, MetaData

now = dt.datetime.now()
todays_date = now.strftime("%D").replace("/", "-")

# SQL DB connection constants
DB_HOSTNAME = f"{my_secrets.debian_dbhost}"
DB_NAME = f"{my_secrets.debian_dbname}"
DB_USER = f"{my_secrets.debian_dbuser}"
DB_PW = f"{my_secrets.debian_dbpass}"

COMMUNITY_RENTAL_TYPES = "community_rental_owner_types"
TOP_RENTAL_TYPES = "top_rental_ownership_type"
TOP_REGISTERED_RENTAL_OWNERS = "top_reg_rental_ownership"
TOP_CLASSED_RENTAL_OWNERS = "top_classed_rental_ownership"
REGISTERED_RENTALS = "registered_rentals"
CLASSED_RENTALS = "classed_rentals"


def check():
    """Check to see if owner and sale triggers are present
    Returns True if both created
    Returns False if either are missing
    False initiates init_db
    """
    logger: Logger = logging.getLogger(__name__)

    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}"
        )
        _meta = MetaData()

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(f"""
            CREATE OR REPLACE
                ALGORITHM = UNDEFINED 
                DEFINER = `hoa_insights`@`%` 
                SQL SECURITY DEFINER
            VIEW `{COMMUNITY_RENTAL_TYPES}` AS
                SELECT 
                    `parcels`.`COMMUNITY` AS `COMMUNITY`,
                    `rentals`.`OWNER_TYPE` AS `OWNER_TYPE`,
                    COUNT('COMMUNITY') AS `total`
                FROM
                    (`rentals`
                    JOIN `parcels` ON ((`rentals`.`APN` = `parcels`.`APN`)))
                GROUP BY `parcels`.`COMMUNITY` , `rentals`.`OWNER_TYPE`
            """)
            )

        logger.info(f"\t{COMMUNITY_RENTAL_TYPES} created")

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(
                    f"""
            CREATE OR REPLACE
                ALGORITHM = UNDEFINED 
                DEFINER = `hoa_insights`@`%` 
                SQL SECURITY DEFINER
            VIEW `{TOP_RENTAL_TYPES}` AS
                SELECT 
                    `rentals`.`OWNER_TYPE` AS `OWNER_TYPE`,
                    COUNT('OWNER_TYPE') AS `count`
                FROM
                    `rentals`
                GROUP BY `rentals`.`OWNER_TYPE`
                ORDER BY `count` DESC
            """
                )
            )

        logger.info(f"\t{TOP_RENTAL_TYPES} created" "")

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    # WEB VIEWS FOR LOCAL WEBSITE

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(
                    f"""
            CREATE OR REPLACE
                ALGORITHM = UNDEFINED 
                DEFINER = `hoa_insights`@`%` 
                SQL SECURITY DEFINER
            VIEW `{REGISTERED_RENTALS}` AS
                SELECT 
                    `rentals`.`APN` AS `APN`,
                    `rentals`.`OWNER` AS `OWNER`,
                    `rentals`.`OWNER_TYPE` AS `OWNER_TYPE`,
                    `rentals`.`CONTACT` AS `CONTACT`,
                    `rentals`.`CONTACT_ADX` AS `CONTACT_ADX`,
                    `rentals`.`CONTACT_PH` AS `CONTACT_PH`,
                    `parcels`.`COMMUNITY` AS `COMMUNITY`,
                    `parcels`.`LAT` AS `LAT`,
                    `parcels`.`LONG` AS `LONG`,
                    `parcels`.`SITUS` AS `SITUS`
                FROM
                    (`rentals`
                    JOIN `parcels` ON ((`rentals`.`APN` = `parcels`.`APN`)))
            """
                )
            )

        logger.info(f"\t{REGISTERED_RENTALS} created" "")

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(
                    f"""
            CREATE OR REPLACE
                ALGORITHM = UNDEFINED
                DEFINER = `hoa_insights`@`%`
                SQL SECURITY DEFINER
            VIEW `{CLASSED_RENTALS}` AS
            SELECT
                `owners`.`OWNER` AS `OWNER`,
                `owners`.`LEGAL_CODE` AS `LEGAL_CODE`,
                `owners`.`MAIL_ADX` AS `MAIL_ADX`,
                `parcels`.`LAT` AS `LAT`,
                `parcels`.`LONG` AS `LONG`,
                `parcels`.`SITUS` AS `SITUS`,
                `parcels`.`APN` AS `APN`,
                `parcels`.`COMMUNITY` AS `COMMUNITY`
            FROM
                (`owners`
                JOIN `parcels` ON ((`owners`.`APN` = `parcels`.`APN`)))
            WHERE
                ((`owners`.`LEGAL_CODE` = '4.2')
                    AND (`owners`.`RENTAL` = 0))                
                    """
                )
            )
        logger.info(f"\t{CLASSED_RENTALS} created" "")

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(
                    f"""
           CREATE OR REPLACE
                ALGORITHM = UNDEFINED 
                DEFINER = `hoa_insights`@`%` 
                SQL SECURITY DEFINER
            VIEW `{TOP_REGISTERED_RENTAL_OWNERS}` AS
            SELECT 
                `registered_rentals`.`OWNER` AS `OWNER`,
                COUNT(`registered_rentals`.`OWNER`) AS `c`
            FROM
                `registered_rentals`
            GROUP BY `registered_rentals`.`OWNER`
            ORDER BY `c` DESC;      
                                """
                )
            )

        logger.info(f"\t{TOP_REGISTERED_RENTAL_OWNERS} created" "")

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(
                    f"""
              CREATE OR REPLACE
                   ALGORITHM = UNDEFINED 
                   DEFINER = `hoa_insights`@`%` 
                   SQL SECURITY DEFINER
               VIEW `{TOP_CLASSED_RENTAL_OWNERS}` AS
               SELECT 
                   `classed_rentals`.`OWNER` AS `OWNER`,
                   COUNT(`classed_rentals`.`OWNER`) AS `c`
               FROM
                   `classed_rentals`
               GROUP BY `classed_rentals`.`OWNER`
               ORDER BY `c` DESC;      
                                   """
                )
            )

        logger.info(f"\t{TOP_CLASSED_RENTAL_OWNERS} created" "")

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    # logger.info(f"""{TOP_CLASSED_RENTAL_OWNERS} {TOP_REGISTERED_RENTAL_OWNERS} {TOP_RENTAL_TYPES}
    #                 {CLASSED_RENTALS}-{REGISTERED_RENTALS}-{COMMUNITY_RENTAL_TYPES} created""")

    return True
