import json
import os
import time
from datetime import datetime
import logging
from cmr import CollectionQuery, GranuleQuery
import traceback
import asyncio
import aiohttp
import aioboto3
from aiobotocore.config import AioConfig
from utils import validate_environment_variables

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

loop = asyncio.get_event_loop()


## =====================================================================================
## Helper functions
## =====================================================================================
def split_date_ranges(start_date, end_date, num_ranges):
    """
    Splits a date range into a specified number of equal subranges.

    Args:
        start_date (str): The start date in ISO format (e.g., "2022-01-01T00:00:00Z").
        end_date (str): The end date in ISO format (e.g., "2022-12-31T23:59:59Z").
        num_ranges (int): The number of subranges to create.

    Returns:
        list of tuples: A list containing tuples of (start, end) date ranges in ISO format.
    """

    start = datetime.fromisoformat(start_date.replace("Z", ""))
    end = datetime.fromisoformat(end_date.replace("Z", ""))
    delta = (end - start) / num_ranges

    return [
        (
            (start + delta * i).isoformat() + "Z",
            (start + delta * (i + 1)).isoformat() + "Z",
        )
        for i in range(num_ranges)
    ]


def build_message(granule, short_name, version):
    """Constructs an SQS message from a granule record."""
    time_start = granule.get("time_start", "")
    time_end = granule.get("time_end", "")
    granule_message = {
        "Message": json.dumps(
            {
                "record": {
                    "beginningDateTime": time_start,
                    "endingDateTime": time_end,
                    "collectionId": f"{short_name}___{version}",
                }
            }
        ),
    }
    return {"Id": granule.get("id", ""), "MessageBody": json.dumps(granule_message)}


def get_params(short_name, version, max_producers=8, consumer_ratio=1.5):
    """Calculates parameters for processing resources based on collection size"""
    try:
        # Get temporal partitions
        api = GranuleQuery()
        api.parameters(short_name=short_name, version=version)
        num_granules = api.hits()

        api = CollectionQuery()
        api.parameters(short_name=short_name, version=version)
        results = api.get_all()

        if not results:
            logger.error(
                f"No collections found for short_name={short_name}, version={version}"
            )
            raise ValueError(
                f"No collections found for short_name={short_name}, version={version}"
            )

        collection = results[0]
        beginning_date = collection.get("time_start")
        ending_date = collection.get("time_end")
        if ending_date is None:
            ending_date = datetime.now().isoformat() + "Z"

        # 1 producer for every 2 CMR queries required for this granule count, up to max_producers
        pages_per_producer = 10
        n_producers = round(
            max(1, min(num_granules / (2000 * pages_per_producer), max_producers))
        )
        n_consumers = round(n_producers * consumer_ratio)
        queue_size = n_producers * 2 * 2000
        date_ranges = split_date_ranges(beginning_date, ending_date, n_producers)

        logger.info(f"Collection {short_name} v{version}: {num_granules} granules, {n_producers} producers, {n_consumers} consumers")
        return date_ranges, n_consumers, queue_size, num_granules

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        logger.debug(traceback.format_exc())
        return None, {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": str(e),
                }
            ),
        }


## =====================================================================================
## Main processing functions
## =====================================================================================
async def fetch_cmr_range(session, url, params, result_queue, fetch_stats):
    """
    Performs paginated requests to the CMR API over a given temporal range using search-after
    and enqueues granule messages into the results queue.

    Args:
        session: aiohttp ClientSession
        url: CMR API endpoint URL
        params: Query parameters for the CMR API request
        result_queue: Queue to place processed granule messages
        fetch_stats: Dictionary to track fetching statistics

    Returns:
        None
    """
    search_after = None
    max_retries = 3
    
    while True:
        headers = {"CMR-Search-After": search_after} if search_after else {}

        for retry in range(max_retries + 1):
            try:
                async with session.get(
                    url, params=params, headers=headers, timeout=60
                ) as response:
                    if response.status != 200:
                        error_body = await response.text()
                        if retry < max_retries:
                            logger.debug(
                                f"CMR API error: HTTP {response.status} on {params}: {error_body} "
                                f"Retrying in {retry ** 2}s ({retry+1}/{max_retries})"
                            )
                            await asyncio.sleep(retry**2)
                            continue
                        else:
                            message = f"Max retries reached: CMR API error: HTTP {response.status} on {params}: {error_body}"
                            logger.error(message)
                            raise Exception(message)

                    data = await response.json()
                    # Get search-after header for next request
                    search_after = response.headers.get("CMR-Search-After")
                    granules = data.get("feed", {}).get("entry", [])
                    if not granules:
                        return
                    # Enqueue message for each granule
                    for i, granule in enumerate(granules):
                        message = build_message(
                            granule, params["short_name"], params["version"]
                        )
                        await result_queue.put(message)
                    fetch_stats["total"] += len(granules)
                    if not search_after:
                        return
                    break

            except Exception as e:
                if retry < max_retries:
                    retry_delay = retry**2
                    logger.debug(
                        f"Error fetching CMR page for {params}: {str(e)}. "
                        f"Retrying in {retry ** 2}s ({retry+1}/{max_retries})"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    message = f"Error fetching CMR page for {params}: {str(e)}. Max retries reached."
                    logger.error(message)
                    raise Exception(message)


async def send_to_sqs(
    sqs_client, short_name, version, result_queue, queue_url, sender_id, send_stats
):
    """
    Consumes messages from the result queue and sends them to SQS in batches.

    Args:
        sqs_client: aioboto3 async SQS client
        short_name: Collection short name
        version: Collection version
        result_queue: Queue containing granule messages to send
        queue_url: URL of the destination SQS queue
        sender_id: Identifier for this sender (for logging)
        send_stats: Dictionary to track sending statistics

    Returns:
        None. Exits when it receives a None message from the queue.
    """
    batch = []
    while True:
        item = await result_queue.get()
        if item is None:
            if batch:
                try:
                    await sqs_client.send_message_batch(
                        QueueUrl=queue_url, Entries=batch
                    )
                    send_stats["total"] += len(batch)
                except Exception as e:
                    logger.error(f"Error sending batch to SQS: {str(e)}")
            break

        batch.append(item)

        if len(batch) >= 10:
            current_batch = batch
            batch = []
            try:
                await sqs_client.send_message_batch(
                    QueueUrl=queue_url, Entries=current_batch
                )
                send_stats["total"] += len(current_batch)
            except Exception as e:
                logger.error(f"Error sending batch to SQS: {str(e)}")
    

async def process_collection(
    partitions,
    short_name,
    version,
    result_queue,
    destination_queue,
    n_consumers,
    total_granules,
):
    """
    Orchestrates collection processing using an async producer-consumer pattern.

    Starts producer tasks to fetch granules from CMR API and consumer tasks
    to send messages to SQS, along with a metrics logger task for monitoring progress.

    Args:
        partitions: List of temporal partition tuples (start_date, end_date)
        short_name: Collection short name
        version: Collection version
        result_queue: Queue for passing granule messages between producers and consumers
        destination_queue: URL of the destination SQS queue
        n_consumers: Number of consumer tasks to create
        total_granules: Total number of granules to process

    Returns:
        None
    """
    lambda_start_time = time.time()
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
    base_params = {
        "short_name": short_name,
        "version": version,
        "page_size": 2000,
    }

    fetch_stats = {"total": 0}
    send_stats = {"total": 0}
    logger.info(f"Starting collection processing: {short_name} v{version} ({len(partitions)} producers, {n_consumers} consumers)")

    async with aiohttp.ClientSession() as http_session:
        async with aioboto3.Session().client(
            "sqs", config=AioConfig(max_pool_connections=n_consumers)
        ) as sqs_client:
            try:
                # Start worker tasks as TaskGroup to cancel all tasks on exception
                async with asyncio.TaskGroup() as tg:
                    # Start a producer on each date range
                    producers = []
                    for i, (start, end) in enumerate(partitions):
                        range_params = base_params.copy()
                        range_params["temporal"] = f"{start},{end}"
                        task = tg.create_task(
                            fetch_cmr_range(
                                http_session,
                                cmr_url,
                                range_params,
                                result_queue,
                                fetch_stats,
                            )
                        )
                        producers.append(task)

                    # Start consumers
                    consumers = []
                    for i in range(n_consumers):
                        task = tg.create_task(
                            send_to_sqs(
                                sqs_client,
                                short_name,
                                version,
                                result_queue,
                                destination_queue,
                                i + 1,
                                send_stats,
                            )
                        )
                        consumers.append(task)

                    # Wait for producers to finish
                    await asyncio.gather(*producers)
                    logger.debug("All producers completed")

                    # Signal consumers to complete
                    for _ in range(n_consumers):
                        await result_queue.put(None)

                    # Wait for consumers
                    await asyncio.gather(*consumers)
                    logger.debug("All consumers completed")

            except Exception as e:
                logger.error(f"Failed to process collection {short_name} v{version}: {e}")
                raise Exception(f"Failed to process collection {short_name} v{version}: {e}")
            finally:
                total_duration = time.time() - lambda_start_time
                throughput = (
                    send_stats["total"] / total_duration if total_duration > 0 else 0
                )
                logger.info(
                    f"Collection processing complete: {short_name} v{version} - "
                    f"{fetch_stats['total']} fetched, {send_stats['total']} sent in {total_duration:.1f}s ({throughput:.1f} msg/s)"
                )


def lambda_handler(event, context):
    """
    AWS Lambda handler that processes an SNS event, retrieves granule data, and sends messages to SQS.

    Args:
        event (dict): The event payload received from SNS.
        context (object): The Lambda context object.

    Returns:
        dict: HTTP response with status code and message.
    """
    # Parse input
    try:
        validate_environment_variables(["QUEUE_URL"])
        destination_queue_url = os.getenv("QUEUE_URL")
        sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
        short_name = sns_message.get("short_name")
        version = sns_message.get("version")
        if not short_name or not version:
            logger.warning("Missing short_name or version in the event")
            return {
                "statusCode": 400,
                "body": json.dumps(
                    "Error: Missing short_name or version in the event."
                ),
            }

    except Exception as e:
        logger.error(f"Input Error: {e}")
        logger.debug(traceback.format_exc())
        return None, {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": str(e),
                }
            ),
        }

    # Determine processing resources based on collection size
    max_producers = 8
    consumer_ratio = 1.5
    temporal_partitions, n_consumers, queue_size, total_granules = get_params(
        short_name,
        version,
    )
    result_queue = asyncio.Queue(queue_size)

    # Process collection
    try:
        loop.run_until_complete(
            process_collection(
                temporal_partitions,
                short_name,
                version,
                result_queue,
                destination_queue_url,
                n_consumers,
                total_granules,
            )
        )
        logger.info(f"Lambda execution completed successfully for {short_name} v{version}")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Processing complete"}),
        }
    except Exception as e:
        logger.error(f"Lambda execution failed for {short_name} v{version}: {str(e)}")
        logger.debug(traceback.format_exc())
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}