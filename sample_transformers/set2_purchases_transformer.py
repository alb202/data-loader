from pandas import DataFrame, to_datetime
from pandera.pandas import DataFrameSchema

# from src.data_loader.transform.transform_base import Transform
from transform.transform_base import Transform


class Transformer(Transform):
    @staticmethod
    def transform(*dfs: DataFrame, output_schema: DataFrameSchema) -> DataFrame:
        # Make a copy of the dataframe
        df_final = dfs[0].copy(deep=True)

        # Rename these columns
        rename_map = {
            "cust_id": "customer_id",
            "item_name": "item_description",
            "cost": "price_paid",
            "sku": "item_code",
            "purchase_channel": "purchase_location",
        }
        df_final = df_final.rename(columns=rename_map)

        # Create a date column from separate values
        df_final["date"] = to_datetime(dict(year=df_final["year"], month=df_final["month"], day=df_final["day"]))

        # Remove prefix from customer ID
        df_final["customer_id"] = df_final["customer_id"].apply(lambda customer_id: customer_id.replace("P-", ""))

        # Make purchase location lowercase
        df_final["purchase_location"] = df_final["purchase_location"].apply(lambda purchase_location: purchase_location.lower())

        # Fill column with null values
        df_final["item_name"] = None

        # Get the columns and sort
        df_final = df_final.loc[:, list(output_schema.columns.keys())]

        # Return dataframe
        return df_final
