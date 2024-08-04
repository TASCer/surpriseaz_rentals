import datetime as dt
import insight_reports
import logging
import my_secrets
import pandas as pd


from datetime import datetime
from logging import Logger
from pandas import DataFrame
from pandas.core.generic import NDFrame
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import Engine

# MAIN SQL DB connection constants
DB_HOSTNAME = f"{my_secrets.debian_dbhost}"
DB_NAME = f"{my_secrets.debian_dbname}"
DB_USER = f"{my_secrets.debian_dbuser}"
DB_PW = f"{my_secrets.debian_dbpass}"

now: datetime = dt.datetime.now()
todays_date: str = now.strftime("%D").replace("/", "-")

ytd_start: str = "2024-01-01"
ytd_end: str = "2025-01-01"


def format_price(price: int) -> str:
    """
    Returns formatted price str in $USD
    ex: 534650 -> $534,650
    """
    price = int(price)
    return "${:,}".format(price)


def get_average_sale_price() -> None:
    """
    Creates mySQL engine to query sales data for all communities
    Creates dataframe of all sale prices and dates in all communities
    Logs YTD Average selling price per community
    """
    logger: Logger = logging.getLogger(__name__)
    try:
        engine: Engine = create_engine(
            f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}"
        )

    except (
        AttributeError,
        exc.SQLAlchemyError,
        exc.OperationalError,
        exc.ProgrammingError,
    ) as e:
        logger.critical(e)
        engine: Engine = None

    with engine.connect() as conn, conn.begin():
        try:
            all_sales_ytd: DataFrame = pd.read_sql(
                f"""SELECT 
				p.COMMUNITY,
				o.SALE_DATE,
				o.SALE_PRICE
				FROM
				owners o 
				INNER JOIN parcels p ON p.APN = o.APN
				where o.SALE_DATE >= '{ytd_start}' and o.SALE_DATE < '{ytd_end}';""",
                conn,
                parse_dates=[1],
                coerce_float=False,
            )

            all_sales_ytd.dropna(inplace=True)

        except (IOError, FileNotFoundError) as e:
            logger.critical(str(e))
            exit()

    all_community_sales_ytd = pd.DataFrame(all_sales_ytd)
    all_community_sales_ytd.to_csv(
        f"{my_secrets.csv_finance_path}all_ytd_community_sales.csv"
    )

    community_sold_count: NDFrame = all_community_sales_ytd.groupby("COMMUNITY").count()
    community_sold_count: NDFrame = community_sold_count.rename(
        columns={"SALE_DATE": "#Sold"}
    )

    ytd_avg_price: DataFrame = all_community_sales_ytd.groupby(["COMMUNITY"]).mean(
        ["SALE_PRICE"]
    )

    ytd_avg_price: DataFrame = ytd_avg_price.rename(columns={"SALE_PRICE": "Avg_Price"})

    ytd_community_avg_sale_price: DataFrame = pd.concat(
        [community_sold_count, ytd_avg_price], axis=1
    )
    del ytd_community_avg_sale_price["SALE_PRICE"]

    ytd_community_avg_sale_price["Avg_Price"] = ytd_community_avg_sale_price[
        "Avg_Price"
    ].apply(format_price)

    ytd_community_avg_sale_price.reset_index(inplace=True)
    ytd_community_avg_sale_price.to_csv(
        f"{my_secrets.csv_finance_path}ytd_community_avg_sale_price.csv"
    )

    insight_reports.financials(ytd_community_avg_sale_price)
