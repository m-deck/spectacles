import requests
from spectacles.client import LookerClient
from spectacles.validators.sql import Query, QueryResult, SqlValidator
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import LookerApiError, SqlError
from typing import Dict, List, Optional
import spectacles.utils as utils
from requests.exceptions import Timeout
from tqdm import tqdm  # type: ignore
import backoff  # type: ignore
import time

TIMEOUT_SEC = 300


class CacheQuery(Query):
    def __init__(
        self,
        query_id: str,
        query_task_id: Optional[str] = None,
    ):
        self.query_id = query_id
        self.query_task_id = query_task_id


class LookerCacheClient(LookerClient):

    # create_query_task is overridden to add cache=True to body
    @backoff.on_exception(backoff.expo, (Timeout,), max_tries=2)
    def create_query_task(self, query_id: int, reset: bool) -> str:
        """Runs a previously created query asynchronously and returns the query task ID.

        If a ClientError or TimeoutError is received, attempts to retry.

        Args:
            session: Existing asychronous HTTP session.
            query_id: ID of a previously created query to run.

        Returns:
            str: ID for the query task, used to check on the status of the query, which
                is being run asynchronously.

        """
        # Using old-style string formatting so that strings are formatted lazily
        logger.debug("Starting query %d", query_id)
        body = {"query_id": query_id, "result_format": "json_detail"}

        url = utils.compose_url(self.api_url, path=["query_tasks"])

        if reset:
            reset_string = "false"  # user-facing value of "reset" indicates whether to bust the cache. API is opposite
        else:
            reset_string = "true"

        response = self.post(
            url=url, json=body, params={"cache": reset_string}, timeout=TIMEOUT_SEC
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-launch-query",
                title="Couldn't launch query.",
                status=response.status_code,
                detail=(
                    "Failed to create query task for "
                    f"query '{query_id}'. Please try again."
                ),
                response=response,
            )

        result = response.json()
        query_task_id = result["id"]
        logger.debug("Query %d is running under query task %s", query_id, query_task_id)
        return query_task_id

    def dashboard(self, dashboard_id: str):
        url = utils.compose_url(self.api_url, path=["dashboards", dashboard_id])
        response = self.get(url=url, timeout=TIMEOUT_SEC)
        return response


def dashboard_ids_to_query_ids(
    client: LookerCacheClient, dashboard_ids_list: List[str]
):

    query_ids_to_cache = []

    print("converting dashboard id list to query id list...")

    for dashboard_id in dashboard_ids_list:
        dashboard_obj = client.dashboard(dashboard_id).json()
        for dashboard_element in dashboard_obj["dashboard_elements"]:
            try:
                if dashboard_element is not None:
                    if dashboard_element["query_id"] is not None:
                        query_ids_to_cache.append(dashboard_element["query_id"])
                else:
                    print("dashboard_element is None, skipping")
            except AttributeError:
                print("Attribute error, skipping dashboard element")

    return query_ids_to_cache


class CacheManager(SqlValidator):

    # __init__ is overridden as project is not required for cache management
    def __init__(self, client: LookerCacheClient, concurrency: int = 10):

        self.client = client
        self.query_slots = concurrency
        self._running_queries: List[Query] = []
        self._query_by_task_id: Dict[str, Query] = {}

    def _fill_query_slots(self, queries: List[CacheQuery], reset: bool) -> None:  # type: ignore[override]
        """Creates query tasks until all slots are used or all queries are running"""
        while queries and self.query_slots > 0:
            logger.debug(
                f"{self.query_slots} available query slots, creating query task"
            )
            query = queries.pop(0)
            query_task_id = self.client.create_query_task(query.query_id, reset)
            self.query_slots -= 1
            query.query_task_id = query_task_id
            self._query_by_task_id[query_task_id] = query
            self._running_queries.append(query)

    def _run_queries(self, queries: List[CacheQuery], reset: bool) -> None:  # type: ignore[override]
        """Creates and runs queries with a maximum concurrency defined by query slots"""
        QUERY_TASK_LIMIT = 250

        logger.info("\n\n" + "Executing queries...")

        len_queries = len(queries)
        progress_bar = tqdm(total=len_queries)

        while queries or self._running_queries:
            if queries:
                logger.debug(f"Starting a new loop, {len(queries)} queries queued")
                self._fill_query_slots(queries, reset)
            query_tasks = self.get_running_query_tasks()[:QUERY_TASK_LIMIT]
            logger.debug(f"Checking for results of {len(query_tasks)} query tasks")
            for query_result in self._get_query_results(query_tasks):
                self._handle_query_result(query_result)
            progress_bar.update(len_queries - len(queries))
            len_queries = len(queries)
            time.sleep(0.5)

        progress_bar.close()

    def touch_cache(self, query_ids_list: List[int], reset: bool):
        queries = [CacheQuery(query_id=str(id)) for id in query_ids_list]

        self._run_queries(queries, reset)

    def _handle_query_result(self, result: QueryResult) -> Optional[SqlError]:
        query = self.get_query_by_task_id(result.query_task_id)
        if result.status in ("complete", "error"):
            self._running_queries.remove(query)
            self.query_slots += 1

            if result.status == "error" and result.error:
                print(str(result.error))
                model_name = "none"
                dimension_name: Optional[str] = None
                explore_name = "none"

                sql_error = SqlError(
                    model=model_name,
                    explore=explore_name,
                    dimension=dimension_name,
                    explore_url="none",
                    lookml_url=None,
                    **result.error,
                )
                return sql_error
        return None
