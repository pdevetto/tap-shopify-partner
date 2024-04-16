"""Stream type classes for tap-shopify-partner."""
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_shopify_partner.client import ShopifyPartnerStream

class TransactionsStream(ShopifyPartnerStream):
    """Define custom stream."""
    name = "transactions"
    query_name = "transactions"
    #TODO automatic discovery using something like
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
    ).to_dict()
    primary_keys = ["id"]
class EventsStream(ShopifyPartnerStream):
    """Define custom stream."""
    name = "events"
    query_name = "events"
    replication_key = "occurredAt"
    replication_key_filter = "occurredAtMin"

    schema = th.PropertiesList(
        th.Property("occurredAt", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("app",th.ObjectType(
            th.Property("id",th.StringType),
            th.Property("name",th.StringType),
        )),
        th.Property("type", th.StringType),
        th.Property("shop",th.ObjectType(
            th.Property("avatarUrl",th.StringType),
            th.Property("id",th.StringType),
            th.Property("myshopifyDomain",th.StringType),
            th.Property("name",th.StringType),
        )),
    ).to_dict()

    @property
    def json_path(self):
        return f"$.data.app"
    
    @property
    def base_query(self)-> str:
        return """
            query tapShopify($first: Int, $after: String,$id: ID!,$occurredAtMin: DateTime) {
                app(id:$id){
                    __query_name__(first: $first, after: $after,occurredAtMin:$occurredAtMin) {
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
            }
        """