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