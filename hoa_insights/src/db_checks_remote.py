# TODO need to add schema check and communities table check for remote site
import datetime as dt
import logging
import my_secrets
import pandas as pd
import sqlalchemy as sa

from logging import Logger
from sqlalchemy import create_engine, exc, types, text, Column, Table, MetaData
from sqlalchemy_utils import database_exists, create_database

now = dt.datetime.now()
todays_date = now.strftime("%D").replace("/", "-")

# BLUEHOST SQL DB connection constants
BH_DB_HOSTNAME = f"{my_secrets.bluehost_dbhost}"
BH_DB_NAME = f"{my_secrets.bluehost_dbname}"
BH_DB_USER = f"{my_secrets.bluehost_dbuser}"
BH_DB_PW = f"{my_secrets.bluehost_dbpass}"

# SQL TABLE constants
COMMUNITY_TOTALS = "communities"


def schema():
    """Check to see if schema/DB_NAME is present, if not, create"""
    logger: Logger = logging.getLogger(__name__)
    try:
        engine = create_engine(
            f"mysql+pymysql://{BH_DB_USER}:{BH_DB_PW}@{BH_DB_HOSTNAME}/{BH_DB_NAME}"
        )

        if not database_exists(engine.url):
            create_database(engine.url)

    except (exc.SQLAlchemyError, exc.OperationalError) as e:
        logger.critical(str(e))
        return False

    return True


def tables():
    """Check to see if all required tables are created
    If not, create them and return True
    Returns False and logs if error in creating
    """
    logger: Logger = logging.getLogger(__name__)

    try:
        engine = create_engine(
            f"mysql+pymysql://{BH_DB_USER}:{BH_DB_PW}@{BH_DB_HOSTNAME}/{BH_DB_NAME}"
        )

    except (exc.SQLAlchemyError, exc.OperationalError) as e:
        logger.critical(str(e))
        return False

    communities_tbl_insp = sa.inspect(engine)
    communities_tbl = communities_tbl_insp.has_table(
        COMMUNITY_TOTALS, schema=f"{BH_DB_NAME}"
    )

    meta = MetaData()

    if not communities_tbl:
        engine = create_engine(
            f"mysql+pymysql://{BH_DB_USER}:{BH_DB_PW}@{BH_DB_HOSTNAME}/{BH_DB_NAME}"
        )

        communities = Table(
            COMMUNITY_TOTALS,
            meta,
            Column("COMMUNITY", types.VARCHAR(100), primary_key=True),
            Column("COUNT", types.INT),
        )
        # TODO how to get local table to renote now?
        # try:
        # 	with engine.connect() as conn, conn.begin():
        # 		q_community_parcel_totals = conn.execute(text(
        # 			"SELECT COMMUNITY, count(COMMUNITY) as COUNT FROM hoa_insights.parcels group by COMMUNITY order by COMMUNITY;"))
        # 		community_total_parcels = [x for x in q_community_parcel_totals]
        # 		community_total_parcels_df = pd.DataFrame(community_total_parcels)
        # 		community_total_parcels_df.to_sql(
        # 			name='communities',
        # 			con=conn,
        # 			if_exists='replace',
        # 			index=False,
        # 			dtype={"COMMUNITY": types.VARCHAR(100)}
        # 		)
        # 		conn.execute(text('alter table communities add primary key(COMMUNITY)'))

        # 	logger.warning(f"Table: {COMMUNITY_TOTALS} did not exist and has been created and seeded")

        # except (IOError, FileNotFoundError, exc.OperationalError, exc.ProgrammingError) as e:
        # 	print(str(f"COMMUNITIES CREATE: " + {e}))

    meta.create_all(engine)

    return True
