"""GraphQL client handling, including ShopifyPartnerStream base class."""
from singer_sdk.streams import GraphQLStream

from time import sleep
from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from backports.cached_property import cached_property
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import GraphQLStream
import math

API_VERSION = '2024-04'

class ShopifyPartnerStream(GraphQLStream):
    """ShopifyPartner stream class."""
    page_size = 1
    query_cost = None
    available_points = None
    restore_rate = None
    max_points = None
    single_object_params = None
    replication_key_filter = None

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"https://partners.shopify.com/{self.config.get('partner_id')}/api/{API_VERSION}/graphql.json"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers['X-Shopify-Access-Token'] = self.config.get("api_key")
        return headers
    
    @property
    def page_size(self) -> int:
        if not self.available_points:
            return 1
        pages = self.available_points / self.query_cost
        if pages < 5:
            points_to_restore = self.max_points - self.available_points
            sleep(points_to_restore // self.restore_rate - 1)
            pages = (self.max_points - self.restore_rate) / self.query_cost
            pages = pages - 1
        elif self.query_cost and pages>5:
            if self.query_cost * pages >= 1000:
                pages = math.floor(1000/self.query_cost)
            else:
                pages = 250 if pages > 250 else pages
        return int(pages)
    @property
    def base_query(self)-> str:
        return """
            query tapShopify($first: Int, $after: String) {
                __query_name__(first: $first, after: $after) {
                    edges {
                        cursor
                        node {
                            __selected_fields__
                        }
                    },
                    pageInfo {
                        hasNextPage
                    }
                }
            }
        """

    #TODO, write automatic query builder
    @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        base_query = self.base_query

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)

        return query
    @property
    def json_path(self):
        return "$.data"
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        """Return token identifying next page or None if all records have been read."""
        if not self.replication_key:
            return None
        response_json = response.json()
        has_next_json_path = f"{self.json_path}.{self.query_name}.pageInfo.hasNextPage"
        has_next = next(extract_jsonpath(has_next_json_path, response_json))
        if has_next:
            cursor_json_path = f"{self.json_path}.{self.query_name}.edges[-1].cursor"
            all_matches = extract_jsonpath(cursor_json_path, response_json)
            return next(all_matches, None)
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params["first"] = self.page_size
        if next_page_token:
            params["after"] = next_page_token
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
                params["filter"] = f"updated_at:>{date}"
                if self.replication_key_filter:
                    params[self.replication_key_filter] = date
        if self.single_object_params:
            params = self.single_object_params
        if self.name == "events":
            params['id'] = f"gid://partners/App/{self.config.get('app_id')}"
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        self.logger.info(f"Parsing response for stream {self.name}")
        if self.replication_key:
            json_path = f"{self.json_path}.{self.query_name}.edges[*].node"
        else:
            json_path = f"{self.json_path}.{self.query_name}"
        response = response.json()
        if "extensions" in response:
            cost = response["extensions"].get("cost")
            if not self.query_cost:
                self.query_cost = cost.get("requestedQueryCost")
            self.available_points = cost["throttleStatus"].get("currentlyAvailable")
            self.restore_rate = cost["throttleStatus"].get("restoreRate")
            self.max_points = cost["throttleStatus"].get("maximumAvailable")

        yield from extract_jsonpath(json_path, input=response)

    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                field_name = key[-1]
                selected_properties.append(field_name)
        return selected_properties

    @property
    def gql_selected_fields(self):
        schema = self.schema["properties"]
        catalog = {k: v for k, v in schema.items() if k in self.selected_properties}

        def denest_schema(schema):
            output = ""
            for key, value in schema.items():
                if "items" in value.keys():
                    value = value["items"]
                if "properties" in value.keys():
                    denested = denest_schema(value["properties"])
                    output = f"{output}\n{key}\n{{{denested}\n}}"
                else:
                    output = f"{output}\n{key}"
            return output

        return denest_schema(catalog)

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the GraphQL API request."""
        params = self.get_url_params(context, next_page_token)
        query = self.query.lstrip()
        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
            "variables": params,
        }
        self.logger.debug(f"Attempting query:\n{query}")
        return request_data    
