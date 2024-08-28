# https://stackoverflow.com/questions/14437293/query-to-check-if-trigger-exist-on-a-mysql-table
import datetime as dt
import logging
import my_secrets

from logging import Logger
from sqlalchemy import create_engine, exc, text, MetaData, select

now = dt.datetime.now()
todays_date = now.strftime("%D").replace("/", "-")

DB_HOSTNAME = f"{my_secrets.debian_dbhost}"
DB_NAME = f"{my_secrets.debian_dbname}"
DB_USER = f"{my_secrets.debian_dbuser}"
DB_PW = f"{my_secrets.debian_dbpass}"


def check() -> bool:
    """Check to see if owner and sale triggers are present
    Returns True if both created
    Returns False if either are missing
    False initiates init_db
    """
    logger: Logger = logging.getLogger(__name__)

    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}")
        _meta = MetaData()

    except exc.SQLAlchemyError as e:
        logger.critical(str(e))
        return False

    with engine.connect() as conn, conn.begin():
        q_owners_trigger = select(text("* from INFORMATION_SCHEMA.TRIGGERS where EVENT_OBJECT_TABLE='owners';"))
        owners_triggers: object = conn.execute(q_owners_trigger)
        owners_triggers: list = [x for x in owners_triggers]

        if owners_triggers:
            return True

        if not owners_triggers:
            try:
                conn.execute(text("DROP TRIGGER IF EXISTS after_sale_update"))
                trig_sales = """CREATE DEFINER=`hoa_insights`@`%` TRIGGER `after_sale_update`
                                AFTER UPDATE ON `owners`
                                FOR EACH ROW BEGIN
                                IF OLD.SALE_DATE <> new.SALE_DATE THEN
                                    INSERT IGNORE into historical_sales(apn,sale_date, sale_price, ts)
                                    VALUES(OLD.APN,OLD.SALE_DATE, OLD.SALE_PRICE, CURRENT_TIME(6))
                                    ON DUPLICATE KEY UPDATE SALE_DATE=OLD.SALE_DATE;
                                END IF;
                            END"""

                conn.execute(text(trig_sales))
                logger.info("TRIGGER: AFTER_SALE_UPDATE has been created")

                trig_owner = """CREATE DEFINER=`hoa_insights`@`%` TRIGGER `after_owner_update`
                            AFTER UPDATE ON `owners`
                            FOR EACH ROW BEGIN
                                IF OLD.OWNER <> new.OWNER THEN
                                    INSERT IGNORE into historical_owners(apn,owner,deed_date,deed_type, ts)
                                    VALUES(OLD.APN,OLD.OWNER,OLD.DEED_DATE,OLD.DEED_TYPE, current_timestamp(6))
                                    ON DUPLICATE KEY UPDATE DEED_DATE=OLD.DEED_DATE;
                                END IF;
    
                                IF OLD.RENTAL = 1 and new.RENTAL = 0 THEN
                                    delete from rentals
                                    where OLD.APN = APN;
    
                                END IF;
                        END"""

                conn.execute(text(trig_owner))
                logger.info("TRIGGER: AFTER_OWNER_UPDATE has been created")

                return True

            except exc.ProgrammingError as e:
                logger.critical(str(e))

                return False
