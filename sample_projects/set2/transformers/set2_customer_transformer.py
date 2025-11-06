from pandas import DataFrame
from datetime import datetime


def transform(*dfs: DataFrame, **kwargs) -> DataFrame:
    # Make a copy of the dataframe
    df_final = dfs[0].copy(deep=True)

    # Rename these columns
    rename_map = {"cust_id": "customer_id", "postal_code": "address_postal_code", "country": "address_country"}
    df_final = df_final.rename(columns=rename_map)

    # Split the address_line into components
    address_parts = df_final["address_line"].str.extract(
        r"(?P<address_street_number>\d+)\s+(?P<address_street_name>[^,]+),\s*(?P<address_city>[^,]+),\s*(?P<address_state>\w{2})"
    )

    # Get the address components
    df_final["address_street_number"] = address_parts["address_street_number"]
    df_final["address_street_name"] = address_parts["address_street_name"]
    df_final["address_city"] = address_parts["address_city"]
    df_final["address_state"] = address_parts["address_state"]

    # Validate the postal code length
    df_final["address_postal_code"] = df_final["address_postal_code"].apply(lambda apc: str(str(apc)[:5])).astype(str)

    # Remove prefix from customer ID
    df_final["customer_id"] = df_final["customer_id"].apply(lambda customer_id: customer_id.replace("P-", ""))

    # Calculate the age
    df_final["age"] = df_final["birth_year"].apply(lambda birth_year: int(datetime.now().year - int(birth_year)))

    # Reformat the sex string
    df_final["sex"] = df_final["sex"].map({"Male": "M", "Female": "F"})

    # Add the name columns
    # df_final["first_name"] = df_final["first_name"]
    # df_final["last_name"] = df_final["last_name"]

    # Get the columns and sort
    df_final = df_final.loc[
        :,
        [
            "customer_id",
            "first_name",
            "last_name",
            "age",
            "sex",
            "address_street_number",
            "address_street_name",
            "address_city",
            "address_state",
            "address_postal_code",
            "address_country",
        ],
    ]

    # Return dataframe
    return df_final
