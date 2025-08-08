"""Stream type classes for tap-shopify-partner."""
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_shopify_partner.client import ShopifyPartnerStream

class TransactionsStream(ShopifyPartnerStream):
    """Define custom stream."""
    name = "transactions"
    query_name = "transactions"
    replication_key = "createdAt"
    primary_keys = ["id"]
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("chargeId", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("__typename", th.StringType),
        th.Property("grossAmount",th.ObjectType(
            th.Property("amount",th.StringType),
            th.Property("currencyCode",th.StringType),  
        )),
        th.Property("netAmount",th.ObjectType(
            th.Property("amount",th.StringType),
            th.Property("currencyCode",th.StringType),
        )),
        th.Property("amount",th.ObjectType(
            th.Property("amount",th.StringType),
            th.Property("currencyCode",th.StringType),
        )),
        th.Property("shopId", th.StringType), 
        th.Property("shopMyshopifyDomain",th.StringType),
        th.Property("shopName",th.StringType),
        th.Property("shopAvatarUrl",th.StringType),
        th.Property("referalCategory", th.StringType),
    ).to_dict()

    @property
    def json_path(self):
        return f"$.data"
    
    @property
    def base_query(self)-> str:
        # Add sub query for getting transaction details for sales (has net- and gross- amount)
        sale_objects = [
            "AppOneTimeSale", 
            "AppSaleAdjustment",
            "AppSaleCredit",
            "AppSubscriptionSale",
            "AppUsageSale",
            "ServiceSale",
            "ServiceSaleAdjustment",
            "ThemeSale",
            "ThemeSaleAdjustment"
        ]
        sales_nodes = " ".join([
            f"""
                ... on {object_name} {{
                    { "chargeId" if object_name in ["AppOneTimeSale", "AppSaleAdjustment", "AppSaleCredit", "AppSubscriptionSale"] else "" }
                    grossAmount {{
                        amount
                        currencyCode
                    }}
                    netAmount {{
                        amount
                        currencyCode
                    }}
                    shop {{
                        shopId: id
                        shopMyshopifyDomain: myshopifyDomain
                        shopName: name
                        shopAvatarUrl: avatarUrl
                    }}
                }}
            """ for object_name in sale_objects
        ])

        # Add sub query for getting transaction details for legacy transaction (only amount)
        transaction_objects = [
            "LegacyTransaction",
            "ReferralAdjustment",
            "ReferralTransaction",
            # "TaxTransaction", # "Field 'shop' doesn't exist on type 'TaxTransaction'"
        ]
        transactions_nodes = " ".join([
            f"""
                ... on {object_name} {{
                    amount {{
                        amount
                        currencyCode
                    }}
                    { "referalCategory: category" if object_name in ["ReferralAdjustment", "ReferralTransaction"] else ""}
                    shop {{
                        shopId: id
                        shopMyshopifyDomain: myshopifyDomain
                        shopName: name
                        shopAvatarUrl: avatarUrl
                    }}
                }}
            """ for object_name in transaction_objects
        ])

        query = f"""
            query tapShopify($first: Int, $after: String, $id: ID!) {{
                transactions(first: $first, after: $after, appId: $id) {{
                    edges {{
                        cursor
                        node {{
                            id
                            createdAt
                            __typename
                            { "\n".join(sales_nodes) }
                            { "\n".join(transactions_nodes) }
                        }}
                    }},
                    pageInfo {{
                        hasNextPage
                    }}
                }}
            }}
        """
        self.logger.info(f"TransactionsStream.base_query = {query}")
        return query

class EventsStream(ShopifyPartnerStream):
    """Define custom stream."""
    name = "events"
    query_name = "events"
    replication_key = "occurredAt"
    replication_key_filter = "occurredAtMin"
    primary_keys = ["occurredAt", "type", "shopId"]

    schema = th.PropertiesList(
        th.Property("occurredAt", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("shopId", th.StringType),
        th.Property("shopMyshopifyDomain",th.StringType),
        th.Property("shopName",th.StringType),
        th.Property("shopAvatarUrl",th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: dict = None) -> dict:
        shop = row.get("shop", {})
        if shop:
            for key in ["shopId", "shopName", "shopMyshopifyDomain", "shopAvatarUrl"]:
                if key not in row and key in shop:
                    row[key] = shop[key]
            row.pop("shop", None)
        return row
    
    @property
    def json_path(self):
        return f"$.data.app"

    @property
    def base_query(self)-> str:
        return """
            query tapShopify($first: Int, $after: String, $id: ID!, $occurredAtMin: DateTime) {
                app(id:$id){
                    __query_name__(first: $first, after: $after, occurredAtMin:$occurredAtMin) {
                        edges {
                            cursor
                            node {
                                occurredAt
                                type
                                shop {
                                    shopId: id
                                    shopMyshopifyDomain: myshopifyDomain
                                    shopName: name
                                    shopAvatarUrl: avatarUrl
                                }
                            }
                        },
                        pageInfo {
                            hasNextPage
                        }
                    }
                }
            }
        """