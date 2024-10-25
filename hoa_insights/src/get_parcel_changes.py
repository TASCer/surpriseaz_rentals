import logging
import my_secrets

from logging import Logger
from sqlalchemy import create_engine, exc, text, TextClause
from utils.date_today import sql_date

# SQL DB connection constants
DB_HOSTNAME: str = f"{my_secrets.debian_dbhost}"
DB_NAME: str = f"{my_secrets.debian_dbname}"
DB_USER: str = f"{my_secrets.debian_dbuser}"
DB_PW: str = f"{my_secrets.debian_dbpass}"


def check() -> tuple[list]:
    """
    Queries historical sales and owner tables for a timestamp of today
    Owner table has a trigger to insert on update
    """
    logger: Logger = logging.getLogger(__name__)
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}")

    with engine.connect() as conn, conn.begin():
        try:
            q_sales: TextClause = conn.execute(
                text(f"""SELECT hs.APN, c.COMMUNITY, hs.SALE_DATE, hs.SALE_PRICE 
                                        from historical_sales 
                                        hs inner join parcels c on hs.APN = c.APN 
                                        where DATE(TS) = '{sql_date()}'""")
            )

            q_owners: TextClause = conn.execute(
                text(f"""SELECT ho.APN, c.COMMUNITY, ho.OWNER, ho.DEED_DATE, ho.DEED_TYPE 
                                         from historical_owners ho 
                                         inner join parcels c on ho.APN = c.APN 
                                         where DATE(TS) = '{sql_date()}'""")
            )

        except exc.OperationalError as e:
            logger.critical(str(e))
            exit()

        sales_updates: list = [x for x in q_sales]
        owners_updates: list = [x for x in q_owners]

    return owners_updates, sales_updates
