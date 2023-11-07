import datetime as dt
import logging
import my_secrets
import pandas as pd

from datetime import datetime
from logging import Logger
from sqlalchemy import create_engine, exc, text

now: datetime = dt.datetime.now()
todays_date: str = now.strftime('%D').replace('/', '-')

# MAIN SQL DB connection constants
DB_HOSTNAME = f'{my_secrets.debian_dbhost}'
DB_NAME = f'{my_secrets.debian_dbname}'
DB_USER = f'{my_secrets.debian_dbuser}'
DB_PW = f'{my_secrets.debian_dbpass}'
# BLUEHOST SQL DB connection constants
BH_DB_HOSTNAME = f'{my_secrets.bluehost_dbhost}'
BH_DB_NAME = f'{my_secrets.bluehost_dbname}'
BH_DB_USER = f'{my_secrets.bluehost_dbuser}'
BH_DB_PW = f'{my_secrets.bluehost_dbpass}'


def web_publish():
    logger: Logger = logging.getLogger(__name__)
    try:
        engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}')
        with engine.connect() as conn, conn.begin():    
            q_registered_rentals = conn.execute(text("""SELECT 
                r.APN,
                p.COMMUNITY,
                r.OWNER,
                r.OWNER_TYPE,
                r.CONTACT,
                r.CONTACT_ADX,
                r.CONTACT_PH,
                p.LAT,
                p.LONG,
                p.SITUS
                FROM
                    parcels p
                INNER JOIN rentals r ON r.APN = p.APN;""")
                                                )

            q_classed_rentals = conn.execute(text("""SELECT
                p.APN, 
                p.COMMUNITY,
                o.OWNER,
                o.LEGAL_CODE,
                o.MAIL_ADX,
                p.LAT,
                p.LONG,
                p.SITUS

                FROM
                    owners o 
                INNER JOIN parcels p ON p.APN = o.APN
                WHERE o.LEGAL_CODE = '4.2' and RENTAL = 0;""")
                                             )
        
        registered: list = [x for x in q_registered_rentals]
        registered_rentals: pd.DataFrame = pd.DataFrame(registered)

        classed: list = [x for x in q_classed_rentals]
        classed_rentals: pd.DataFrame = pd.DataFrame(classed)
    
        logger.info(f'Registered Rentals: {len(registered_rentals)} - Classed Rentals - {len(classed_rentals)}')

    except exc.DBAPIError as e:
            logger.error(str(e))


    # PUBLISH to BLUEHOST 
    try:
        logger: Logger = logging.getLogger(__name__)
        engine = create_engine(f'mysql+pymysql://{BH_DB_USER}:{BH_DB_PW}@{BH_DB_HOSTNAME}/{BH_DB_NAME}')
        with engine.connect() as conn, conn.begin():
            try:
                registered_rentals.to_sql(name='all_registered_rentals',
                            con=conn,
                            if_exists='replace',
                            index=False,
                            )
                logger.info(f"Table 'all_registered_rentals' has been updated REMOTELY")
    
    
                classed_rentals.to_sql(name='all_classed_rentals',
                            con=conn,
                            if_exists='replace',
                            index=False,
                            )
                logger.info(f"Table: 'all_classed_rentals' has been updated REMOTELY")
    
                pd.Series(now).to_sql(name='last_updated',
                            con = conn,
                            if_exists='replace',
                            index=False,
                            )
                logger.info(f"Table: 'last_updated' has been updated REMOTELY")
 
            except exc.SQLAlchemyError as e:
                logger.critical(repr(e))
    
    except exc.OperationalError as e:
        logger.critical(repr(e))
        