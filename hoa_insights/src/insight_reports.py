import logging
import my_secrets
import os
import pdfkit as pdf
import platform
import shutil
import styles

from logging import Logger
from mailer import send_mail
from pandas.io.formats.style import Styler
from utils.parsers import format_price
from utils.date_today import log_date

logger: Logger = logging.getLogger(__name__)


def parcel_changes(parcel_changes: object) -> None:
    """Takes in latest parcel change file
    Produces .html report
    Securely sends the .html report to web server
    """
    parcel_changes["SALE_PRICE"] = (parcel_changes["SALE_PRICE"].fillna(0).astype(int).apply(format_price))
    parcel_changes["SALE_DATE"] = parcel_changes["SALE_DATE"].fillna("")

    parcel_changes = parcel_changes.reset_index()

    parcel_changes_caption: str = (f"HISTORICAL COMMUNITY PARCEL INSIGHTS  <br> As Of: {log_date()}")

    parcel_changes_style: Styler = (
        parcel_changes.style.set_table_styles(styles.get_style_changes())
        .set_caption(parcel_changes_caption)
        .hide(axis="index")
    )
    parcel_changes_report: str = f"{my_secrets.html_changes_path}recent_changes.html"
    parcel_changes_style.to_html(parcel_changes_report)

    if not platform.system() == "Windows":
        try:
            os.system(f"scp {parcel_changes_report} {my_secrets.web_server_path_linux}")
            logger.info(f"{parcel_changes_report.split('/')[-1]} sent to tascs.test web server")

        except BaseException:
            logger.critical(f"{parcel_changes_report} NOT sent to tascs.test web server. Investigate")
    
    else:
        try:
            shutil.copy(parcel_changes_report, my_secrets.web_server_path_windows)

        except (IOError, FileNotFoundError) as e:
            logger.error(e)

    # TO PDF and email
    pdf.from_file(parcel_changes_report, "../output/pdf/recent_changes.pdf")


def financials(community_avg_prices) -> None:
    """Takes in a df
    Produces .html reports
    Sends .html reports to web server for display
    Emails reports to users
    """

    finance_caption: str = f"YTD AVERAGE SALES PRICE<br> PROCESSED: {log_date()}"

    finance_style: Styler = (
        community_avg_prices.style.set_table_styles(styles.get_style_finance())
        .set_caption(finance_caption)
        .hide(axis="index")
    )

    finance_report: str = f"{my_secrets.html_finance_path}community_ytd_sales_avg.html"

    finance_style.to_html(finance_report)

    if not platform.system() == "Windows":
        try:
            os.system(f"scp {finance_report} {my_secrets.web_server_path_linux}")
            logger.info(f"\t{finance_report.split('/')[-1]} sent to tascs.test web server")

        except BaseException:
            logger.critical(f"{finance_report} NOT sent to tascs.test web server. Investigate")

    else:
        try:
            shutil.copy(finance_report, my_secrets.web_server_path_windows)

        except (IOError, FileNotFoundError) as e:
            logger.error(e)

    # Add TO PDF. Need to download and install for windows, had to apt for linux https://github.com/JazzCore/python-pdfkit/wiki/Installing-wkhtmltopdf
    pdf.from_file(finance_report, "../output/pdf/community_ytd_sales_avg.pdf")

    # TO email
    report_attachment: str = (f"{my_secrets.html_finance_path}community_ytd_sales_avg.html")

    try:
        send_mail("COMMUNITY YTD AVG SALES", report_attachment)
        logger.info("\tCOMMUNITY YTD AVG SALES pdf emailed")
        
    except BaseException as e:
        logger.exception(str(e))
