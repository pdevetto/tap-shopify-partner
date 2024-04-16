"""ShopifyPartner tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_shopify_partner.streams import (
    ShopifyPartnerStream,
    TransactionsStream,
    EventsStream,
    
)

STREAM_TYPES = [
    TransactionsStream,
    EventsStream,
]


class TapShopifyPartner(Tap):
    """ShopifyPartner tap class."""
    name = "tap-shopify-partner"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "partner_id",
            th.IntegerType,
            required=True,
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
if __name__ == '__main__':
    TapShopifyPartner.cli()    
