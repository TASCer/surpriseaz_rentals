import datetime as dt
import db_checks
import db_checks_remote
import get_parcel_changes
import get_parcel_data
import insight_reports
import logging
import mailer
import my_secrets
import pandas as pd
import publish_rental_insights
import triggers_checks
import update_parcel_data
import views_checks

from datetime import date
from financials import ytd_sales
from logging import Logger, Formatter
from pandas import DataFrame
from tests import test_get_parcel_data
from tests import test_update_parcel_data

now: date = dt.date.today()
todays_date: str = now.strftime('%D').replace('/', '-')

root_logger: Logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

fh = logging.FileHandler(f'../log{todays_date}.log')
fh.setLevel(logging.DEBUG)

formatter: Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

root_logger.addHandler(fh)

TESTING: bool = False


def start_insights() -> list[object]:
    """Gathers information on parcels and returns list of each parcels current data"""
    if not TESTING:
        logger.info(f"********** HOA INSIGHT PROCESSING STARTED **********")
        results = get_parcel_data.process_api()
    else:
        logger.info(f"********** [TESTING] HOA INSIGHT PROCESSING STARTED **********")
        test_get_parcel_data.process_json()
        results = test_update_parcel_data.process_json()

    return results


def get_new_insights() -> DataFrame:
    """ Gets recent parcel changes by querying historical sales and owner tables for timestamp: today
        Creates a merged dataframe of changes that outputs to csv
        Returns dataframe of changes to parcel(s)
    """
    owner_updates, sale_updates = get_parcel_changes.check()
    owner_update_count: int = len(owner_updates)
    sale_update_count: int = len(sale_updates)
    
    if sale_update_count >= 1:
        ytd_sales.get_average_sale_price()
    
    if owner_update_count >= 1 or sale_update_count >= 1:
        logger.info(f'New Owners: {len(owner_updates)} - New Sales: {len(sale_updates)}')
        owner_changes = DataFrame(owner_updates, columns=['APN', 'COMMUNITY', 'OWNER', 'DEED_DATE', 'DEED_TYPE']).set_index(['APN'])
        sale_changes = DataFrame(sale_updates, columns=['APN', 'COMMUNITY', 'SALE_DATE', 'SALE_PRICE']).set_index('APN')
        all_changes: DataFrame = owner_changes.merge(sale_changes, how='outer', on=['APN'], sort=True, suffixes=('', '_y'))
        all_changes.drop(all_changes.filter(regex='_y$').columns, axis=1, inplace=True)
        all_changes.to_csv(f"{my_secrets.csv_changes_path}{todays_date}.csv")

        return all_changes

    else:
        return DataFrame()


if __name__ == '__main__':
    logger: Logger = logging.getLogger(__name__)
    logger.info("Checking RDBMS Availability")
    have_database: bool = db_checks.schema()
    have_tables: bool = db_checks.tables()
    have_triggers: bool = triggers_checks.check()
    have_views: bool = views_checks.check()

    if have_database and have_triggers and have_tables and have_views:
        logger.info(f"RDMS: {have_database} | TRIGGERS: {have_triggers} | TABLES: {have_tables} | VIEWS: {have_views }")
        latest_parcel_data = start_insights()
        update_parcel_data.update(latest_parcel_data)
        if not TESTING:
            have_database_remote: bool = db_checks_remote.schema()
            have_tables_remote: bool = db_checks_remote.tables()
            if have_database_remote and have_tables_remote:
                publish_rental_insights.web_publish()
    
        parcel_changes: DataFrame = get_new_insights()

        if not parcel_changes.empty:
            insight_reports.parcel_changes(parcel_changes)

        else:
            logger.info("NO SALES OR OWNER CHANGES")

    else:
        logger.error(f"RDMS: {have_database} | TRIGGERS: {have_triggers} | TABLES: {have_tables} | VIEWS: {have_views }")

    mailer.send_mail("HOA INSIGHTS PROCESSING COMPLETE")
    
    logger.info(f"********** HOA INSIGHT PROCESSING COMPLETED **********")
