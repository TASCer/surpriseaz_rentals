import db_checks
import db_checks_remote
import get_parcel_changes
import fetch_parcel_data
import insight_reports
import logging
import mailer
import my_secrets
import publish_rental_insights
import triggers_checks
import update_parcel_data
import views_checks

from financials import ytd_sales
from logging import Logger, Formatter
from pandas import DataFrame
from tests import test_get_parcel_data
from tests import test_update_parcel_data
from utils.date_today import log_date


root_logger: Logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

fh = logging.FileHandler(f"../{log_date()}.log")
fh.setLevel(logging.DEBUG)

formatter: Formatter = logging.Formatter("%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s")

fh.setFormatter(formatter)

root_logger.addHandler(fh)

TESTING: bool = False


def start_insights() -> list[object]:
    """
    If not testing, gathers parcel data via MARICOPA AZ ACCESSOR API and returns list of each parcels current data
    If testing, gathers parcel data via JSON files and returns list of each parcels data
    """
    if not TESTING:
        logger.info("********** HOA INSIGHT PROCESSING STARTED **********")
        results = fetch_parcel_data.process_api()
    else:
        logger.info("********** [TESTING] HOA INSIGHT PROCESSING STARTED **********")
        test_get_parcel_data.process_json()
        results = test_update_parcel_data.process_json()

    return results


def get_new_insights() -> DataFrame:
    """
    Gets recent parcel changes by querying historical sales and owner tables for timestamp: today
    Creates a merged dataframe of changes that outputs to csv
    Returns dataframe of parcel(s) changes or an empty dataframe if no changes
    """
    owner_updates, sale_updates = get_parcel_changes.check()
    owner_update_count: int = len(owner_updates)
    sale_update_count: int = len(sale_updates)

    if sale_update_count >= 1:
        ytd_sales.get_average_sale_price()

    if owner_update_count >= 1 or sale_update_count >= 1:
        logger.info(f"\tNew Owners: {len(owner_updates)} - New Sales: {len(sale_updates)}")

        owner_changes: DataFrame = DataFrame(owner_updates, columns=["APN", "COMMUNITY", "OWNER", "DEED_DATE", "DEED_TYPE"]).set_index(["APN"])
        sale_changes: DataFrame = DataFrame(sale_updates, columns=["APN", "COMMUNITY", "SALE_DATE", "SALE_PRICE"]).set_index("APN")
        
        all_changes: DataFrame = owner_changes.merge(sale_changes, how="outer", on=["APN"], sort=True, suffixes=("", "_y"))
        all_changes.drop(all_changes.filter(regex="_y$").columns, axis=1, inplace=True)
        all_changes.to_csv(f"{my_secrets.csv_changes_path}{log_date()}.csv")

        return all_changes

    else:
        return DataFrame()


def main() -> None:
    latest_parcel_data: DataFrame = start_insights()
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

    mailer.send_mail("HOA INSIGHTS PROCESSING COMPLETE")

    logger.info("********** HOA INSIGHT PROCESSING COMPLETED **********")


if __name__ == "__main__":
    logger: Logger = logging.getLogger(__name__)
    logger.info("Checking RDBMS Availability")
    have_database: bool = db_checks.schema()
    have_tables: bool = db_checks.tables()
    have_triggers: bool = triggers_checks.check()
    have_views: bool = views_checks.check()

    if have_database and have_triggers and have_tables and have_views:
        logger.info(f"RDMS: {have_database} | TRIGGERS: {have_triggers} | TABLES: {have_tables} | VIEWS: {have_views }")

        main()

    else:
        logger.error(f"RDMS: {have_database} | TRIGGERS: {have_triggers} | TABLES: {have_tables} | VIEWS: {have_views }")

        exit()
