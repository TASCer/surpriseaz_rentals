import aiohttp
import asyncio
import json
import logging
import my_secrets
import platform

from aiohttp import TCPConnector
from aiohttp_retry import RetryClient, ExponentialRetry
from asyncio import Semaphore, Task
from logging import Logger
from sqlalchemy import create_engine, exc, text, TextClause, Row
from typing import Sequence

logger: Logger = logging.getLogger(__name__)

# SQL DB connection constants
DB_HOSTNAME: str = f"{my_secrets.debian_dbhost}"
DB_NAME: str = f"{my_secrets.debian_dbname}"
DB_USER: str = f"{my_secrets.debian_dbuser}"
DB_PW: str = f"{my_secrets.debian_dbpass}"
# SQL Table names
COMMUNITY_NAMES: str = "communities"
PARCEL_CONSTANTS: str = "parcels"
PARCEL_OWNERS: str = "owners"
PARCEL_SALES: str = "sales"
PARCEL_RENTALS: str = "rentals"

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

API_HEADER: dict[str, str] = {my_secrets.api_header_type: my_secrets.api_header_creds}


def get_parcel_apns() -> list[str]:
    """Collects and returns all Accessor Parcel Numbers from database for API processing"""
    try:
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOSTNAME}/{DB_NAME}")

        with engine.connect() as conn, conn.begin():
            results: TextClause = conn.execute(text(f"SELECT APN FROM {DB_NAME}.{PARCEL_CONSTANTS};"))
            APNs: Sequence[Row] = results.all()
            APNs: list[str] = [x[0] for x in APNs]

        return APNs

    except exc.DBAPIError as e:
        logger.error(str(e))


def process_api() -> list[object]:
    """Iterate through each community parcel getting the latest data from API."""
    APNS: list = get_parcel_apns()
    logger.info("\tAccessing Assessor API to get latest insights")
    consumed_parcel_data: object = asyncio.run(async_main(APNS))
    logger.info("\tAll latest parcel data consumed from API")

    return consumed_parcel_data


async def get_parcel_details(client: RetryClient, sem: Semaphore, url: str) -> dict:
    """Takes an api client, semaphore, and API url to get latest parcel data"""
    try:
        async with sem, client.get(url) as resp:
            parcel_details: object = await resp.json(encoding="UTF-8", content_type="application/json")

            return parcel_details

    except (
        json.JSONDecodeError,
        aiohttp.client.ClientOSError,
        aiohttp.client.ContentTypeError,
        aiohttp.ClientResponseError,
        TypeError,
        aiohttp.ClientPayloadError,
    ) as e:
        logger.warning(f"{e} - {url}")

        await asyncio.sleep(4)

        async with sem, client.get(url) as resp:
            parcel_details = await resp.json(encoding="UTF-8", content_type="application/json")

        return parcel_details


async def async_main(APNS: list[str]) -> tuple[object]:
    """
    Takes in a list of APN's
    Creates ACCESSOR API connection/session and retry client
    Iterates through list of APNs creating get_parcel_details tasks
    Returns a list of dictionary objects for each APN/parcel processed
    """
    connector: TCPConnector = TCPConnector(ssl=False, limit=40, limit_per_host=40, enable_cleanup_closed=False)

    async with RetryClient(
        headers=API_HEADER,
        connector=connector,
        raise_for_status=False,
        retry_options=ExponentialRetry(attempts=3),
    ) as retry_client:
        sem: Semaphore = asyncio.Semaphore(40)
        tasks: list[Task[object]] = []
        for apn in APNS:
            parcel_url: str = f"https://mcassessor.maricopa.gov/parcel/{apn}"
            tasks.append(asyncio.create_task(get_parcel_details(retry_client, sem, parcel_url)))

        parcels: tuple[object] = await asyncio.gather(*tasks, return_exceptions=False)

        return parcels
