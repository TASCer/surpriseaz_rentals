import datetime as dt
import logging
import my_secrets
import pandas as pd
import sqlalchemy as sa

from logging import Logger
from sqlalchemy import create_engine, exc, types, text, Column, Table, MetaData
from sqlalchemy_utils import database_exists, create_database

now = dt.datetime.now()
todays_date = now.strftime('%D').replace('/', '-')

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


# SQL TABLE constants
COMMUNITY_TOTALS = 'communities'
PARCELS = 'parcels'
# SQL TABLE names
OWNERS = 'owners'
RENTALS = 'rentals'
SALES_HISTORY = 'historical_sales'
OWNERS_HISTORY = 'historical_owners'


def schema():
	"""Check to see if schema/DB_NAME is present, if not, create"""
	logger: Logger = logging.getLogger(__name__)
	try:
		engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}')

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
		engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}')

	except (exc.SQLAlchemyError, exc.OperationalError) as e:
		logger.critical(str(e))
		return False

	communities_tbl_insp = sa.inspect(engine)
	communities_tbl = communities_tbl_insp.has_table(COMMUNITY_TOTALS, schema=f"{DB_NAME}")
	parcels_tbl_insp = sa.inspect(engine)
	parcels_tbl = parcels_tbl_insp.has_table(PARCELS, schema=f"{DB_NAME}")
	owners_tbl_insp = sa.inspect(engine)
	owners_tbl = owners_tbl_insp.has_table(OWNERS, schema=f"{DB_NAME}")
	rentals_tbl_insp = sa.inspect(engine)
	rentals_tbl = rentals_tbl_insp.has_table(RENTALS, schema=f"{DB_NAME}")
	historical_sales_insp = sa.inspect(engine)
	historical_sales_tbl = historical_sales_insp.has_table(SALES_HISTORY, schema=f"{DB_NAME}")
	historical_owners_insp = sa.inspect(engine)
	historical_owners_tbl = historical_owners_insp.has_table(OWNERS_HISTORY, schema=f"{DB_NAME}")

	meta = MetaData()

	if not parcels_tbl:
		try:
			parcels = Table(
				PARCELS, meta,
				Column('APN', types.VARCHAR(11), primary_key=True),
				Column('COMMUNITY', types.VARCHAR(100)),
				Column('SITUS', types.VARCHAR(100)),
				Column('LAT', types.INT),
				Column('LONG', types.INT)
			)
		except (exc.SQLAlchemyError, exc.ProgrammingError, exc.OperationalError) as e:
			logger.error(str(e))
			return False

		try:
			parcel_constants = pd.read_csv(f"../input/{PARCELS}.csv", index_col=0, header=0, skiprows=None)
		except IOError as e:
			logger.error(e)
			return False

		with engine.connect() as conn, conn.begin():
			try:
				parcel_constants.to_sql(
					name=PARCELS,
					con=conn,
					if_exists='replace',
					index=True,
					dtype={"APN": sa.types.VARCHAR(length=11)}
				)
				conn.execute(text('alter table parcels add primary key(APN)'))
				logger.warning(f"Table: {PARCELS} did not exist and has been created and seeded")

			except (IOError, FileNotFoundError) as e:
				logger.critical(str(e))
				return False

	if not communities_tbl:
		engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}')

		communities = Table(
			COMMUNITY_TOTALS, meta,
			Column('COMMUNITY', types.VARCHAR(100), primary_key=True),
			Column('COUNT', types.INT)
		)
		try:
			with engine.connect() as conn, conn.begin():
				q_community_parcel_totals = conn.execute(text(
					"SELECT COMMUNITY, count(COMMUNITY) as COUNT FROM hoa_insights.parcels group by COMMUNITY order by COMMUNITY;"))
				community_total_parcels = [x for x in q_community_parcel_totals]
				community_total_parcels_df = pd.DataFrame(community_total_parcels)
				community_total_parcels_df.to_sql(
					name='communities',
					con=conn,
					if_exists='replace',
					index=False,
					dtype={"COMMUNITY": types.VARCHAR(100)}
				)
				conn.execute(text('alter table communities add primary key(COMMUNITY)'))

			logger.warning(f"Table: {COMMUNITY_TOTALS} did not exist and has been created and seeded")

		except (IOError, FileNotFoundError, exc.OperationalError, exc.ProgrammingError) as e:
			print(str(f"COMMUNITIES CREATE: " + {e}))

	if not owners_tbl:
		owners = Table(
			OWNERS, meta,
			Column('APN', types.VARCHAR(11), primary_key=True),
			Column('OWNER', types.VARCHAR(120)),
			Column('MAIL_ADX', types.VARCHAR(120)),
			Column('SALE_DATE', types.Date),
			Column('SALE_PRICE', types.INT),
			Column('DEED_DATE', types.Date),
			Column('DEED_TYPE', types.VARCHAR(3)),
			Column('LEGAL_CODE', types.VARCHAR(3)),
			Column('RENTAL', types.INT)
		)
		logger.warning(f"Table: {OWNERS} did not exist and has been created")

	if not rentals_tbl:
		rentals = Table(
			RENTALS, meta,
			Column('APN', types.VARCHAR(11), primary_key=True),
			Column('OWNER', types.VARCHAR(120)),
			Column('OWNER_TYPE', types.VARCHAR(40)),
			Column('CONTACT', types.VARCHAR(120)),
			Column('CONTACT_ADX', types.VARCHAR(120)),
			Column('CONTACT_PH', types.VARCHAR(120))
		)
		logger.warning(f"Table: {RENTALS} did not exist and has been created")

	if not historical_sales_tbl:
		historical_sales = Table(
			SALES_HISTORY, meta,
			Column('APN', types.VARCHAR(11), primary_key=True),
			Column('SALE_DATE', types.DATE, primary_key=True, default="1901-01-01"),
			Column('SALE_PRICE', types.INT),
			Column('TS', types.TIMESTAMP(6))
		)
		logger.warning(f"Table: {SALES_HISTORY} did not exist and has been created")

	if not historical_owners_tbl:
		historical_owners = Table(
			OWNERS_HISTORY, meta,
			Column('APN', types.VARCHAR(11), primary_key=True),
			Column('OWNER', types.VARCHAR(255), primary_key=True),
			Column('DEED_DATE', types.DATE),
			Column('DEED_TYPE', types.VARCHAR(20)),
			Column('TS', types.TIMESTAMP(6))
		)
		logger.warning(f"Table: {OWNERS_HISTORY} did not exist and has been created")

	meta.create_all(engine)

	# LOCAL 'COMMUNITIES' tables
	# try:
	# 	engine = create_engine(f'mysql+pymysql://{my_secrets.rentals_dbuser}:{my_secrets.rentals_dbpass}@{my_secrets.rentals_dbhost}/{my_secrets.rentals_dbname}')
	# 	if not database_exists(engine.url):
	# 		create_database(engine.url)

	# 	communities_tbl_insp = sa.inspect(engine)
	# 	communities_tbl = communities_tbl_insp.has_table(COMMUNITY_TOTALS, schema="tascsnet_insights")

	# 	if not communities_tbl:
	# 		with engine.connect() as conn, conn.begin():
	# 			q_community_parcel_totals = conn.execute(text("SELECT COMMUNITY, count(COMMUNITY) as COUNT FROM hoa_insights.parcels group by COMMUNITY order by COMMUNITY;"))
	# 			community_total_parcels = [x for x in q_community_parcel_totals]
	# 			community_total_parcels_df = pd.DataFrame(community_total_parcels)
	# 			community_total_parcels_df.to_sql(name='communities', con=conn, if_exists='replace', index=False)
	# 		logger.info(f"Table: {COMMUNITY_TOTALS} has been created on {my_secrets.rentals_dbname}")

	# except exc.SQLAlchemyError as e:
	# 	logger.critical(str(e))

	# 	return False

	# BH WEBSITE 'COMMUNITIES' tables
	# try:
	# 	engine = create_engine(f'mysql+pymysql://{BH_DB_USER}:{BH_DB_PW}@{BH_DB_HOSTNAME}/{BH_DB_NAME}')
	# 	if not database_exists(engine.url):
	# 		create_database(engine.url)
	
	# 	communities_tbl_insp = sa.inspect(engine)
	# 	communities_tbl = communities_tbl_insp.has_table(COMMUNITY_TOTALS, schema="tascsnet_insights")
	
	# 	if not communities_tbl:
	# 		with engine.connect() as conn, conn.begin():
	# 			q_community_parcel_totals = conn.execute(text("SELECT COMMUNITY, count(COMMUNITY) as COUNT FROM tascsnet_insights.parcel_constants group by COMMUNITY order by COMMUNITY;"))
	# 			community_total_parcels = [x for x in q_community_parcel_totals]
	# 			community_total_parcels_df = pd.DataFrame(community_total_parcels)
	# 			community_total_parcels_df.to_sql(name='communities',
	# 			con=conn,
	# 			if_exists='replace',
	# 			index=False,
	# 			)
	# 		logger.info(f"Table: {COMMUNITY_TOTALS} has been created on Bluehost")
	
	# except exc.SQLAlchemyError as e:
	# 	logger.critical(str(e))

	return True
