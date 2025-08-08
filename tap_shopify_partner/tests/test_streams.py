from unittest.mock import Mock
from tap_shopify_partner.streams import TransactionsStream
from tap_shopify_partner.tap import TapShopifyPartner

def test_transaction_stream_query():
    tap = Mock()
    tap.config = {}
    stream = TransactionsStream(tap)

    print(stream.base_query)
    assert stream.base_query != ""
