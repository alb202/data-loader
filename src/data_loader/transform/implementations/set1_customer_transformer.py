from pandas import DataFrame
from pandera.pandas import DataFrameSchema

# from src.data_loader.transform.transform_base import Transform
from transform.transform_base import Transform


class Transformer(Transform):
    """Implementation of transformer class"""

    @staticmethod
    def transform(*dfs: DataFrame, output_schema: DataFrameSchema) -> DataFrame:
        # Make a copy of the dataframes
        df0 = dfs[0].copy(deep=True)
        df1 = dfs[1].copy(deep=True)

        df_final = df0.merge(df1, how="outer", on="person_id")

        # Rename these columns
        rename_map = {
            "person_id": "customer_id",
            "address": "address_full",
            "city": "address_city",
            "state": "address_state",
            "zip_code": "address_postal_code",
            "gender": "sex",
        }
        df_final = df_final.rename(columns=rename_map)

        # Split the address_line into components
        address_parts = df_final["address_full"].str.extract(r"(?P<address_street_number>\d+)\s+(?P<address_street_name>[^,]+)")

        # Split the address_line into components
        name_parts = df_final["full_name"].str.extract(r"(?P<first_name>\d+)\s+(?P<last_name>[^,]+)")

        # Get the address components
        df_final["address_street_number"] = address_parts["address_street_number"]
        df_final["address_street_name"] = address_parts["address_street_name"]

        # Get the address components
        df_final["first_name"] = name_parts["first_name"]
        df_final["last_name"] = name_parts["last_name"]

        # Validate the postal code length
        df_final["address_postal_code"] = df_final["address_postal_code"].apply(lambda apc: str(str(apc)[:5])).astype(str)

        # Add the country. Assume USA
        df_final["address_country"] = "USA"

        # Remove prefix from customer ID
        df_final["customer_id"] = df_final["customer_id"].apply(lambda customer_id: customer_id.replace("C", ""))

        # # Calculate the age
        # df_final["age"] = df_final["birth_year"].apply(lambda birth_year: int(datetime.now().year - int(birth_year)))

        # # Reformat the sex string
        # df_final["sex"] = df_final["sex"].map({"Male": "M", "Female": "F"})

        # Get the columns and sort
        df_final = df_final.loc[:, list(output_schema.columns.keys())]

        # Return dataframe
        return df_final
