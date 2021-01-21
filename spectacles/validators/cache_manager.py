import requests
from spectacles.client import LookerClient
from spectacles.validators.sql import Query, QueryResult, SqlValidator
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import LookerApiError, SqlError
from spectacles.lookml import Dimension, Explore
from typing import Dict, List, Optional, Union, Any
import spectacles.utils as utils
from requests.exceptions import Timeout
from tqdm import tqdm  # type: ignore
import backoff  # type: ignore

TIMEOUT_SEC = 300


class CacheQuery(Query):
    def __init__(
            self,
            query_id: str,
            #lookml_ref: Union[Dimension, Explore],
            explore_url: Optional[str] = None,
            query_task_id: Optional[str] = None,
    ):
        self.query_id = query_id
        #self.lookml_ref = lookml_ref
        self.explore_url = explore_url
        self.query_task_id = query_task_id


class LookerCacheClient(LookerClient):

    # create_query_task is overridden to add cache=True to body
    @backoff.on_exception(backoff.expo, (Timeout,), max_tries=2)
    def create_query_task(self, query_id: int) -> str:
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

        response = self.post(
            url=url, json=body, params={"cache": "true"}, timeout=TIMEOUT_SEC
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


def dashboard_ids_to_query_ids(client: LookerCacheClient, dashboard_ids_list: List[str]):

    query_ids_to_cache = []

    print("converting dashboard id list to query id list...")

    for dashboard_id in tqdm(dashboard_ids_list):
        dashboard_obj = client.dashboard(dashboard_id).json()
        for dashboard_element in tqdm(dashboard_obj['dashboard_elements']):
            try:
                if dashboard_element is not None:
                    if dashboard_element['query_id'] is not None:
                        query_ids_to_cache.append(dashboard_element['query_id'])
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

    #def query_ids_to_query_objs(self, query_ids_list: List[int]):

    def touch_cache(self, query_ids_list: List[int]):
        queries = [CacheQuery(query_id=str(id)) for id in query_ids_list]

        self._run_queries(queries)

    def _handle_query_result(self, result: QueryResult) -> Optional[SqlError]:
        query = self.get_query_by_task_id(result.query_task_id)
        if result.status in ("complete", "error"):
            self._running_queries.remove(query)
            self.query_slots += 1
            lookml_object = query.lookml_ref
            #lookml_object.queried = True

            if result.status == "error" and result.error:
                print(str(result.error))
                model_name = "none"#lookml_object.model_name
                dimension_name: Optional[str] = None
                if isinstance(lookml_object, Dimension):
                    explore_name = lookml_object.explore_name
                    dimension_name = lookml_object.name
                else:
                    explore_name = "none"#lookml_object.name

                sql_error = SqlError(
                    model=model_name,
                    explore=explore_name,
                    dimension=dimension_name,
                    explore_url=query.explore_url,
                    lookml_url=getattr(lookml_object, "url", None),
                    **result.error,
                )
                #lookml_object.errors.append(sql_error)
                return sql_error
        return None




