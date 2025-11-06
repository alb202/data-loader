from pandas import DataFrame, to_datetime


def transform(*dfs: DataFrame, **kwargs) -> DataFrame:
    # Make a copy of the dataframe
    df_final = dfs[0].copy(deep=True)

    # Rename these columns
    rename_map = {
        "person_id": "customer_id",
        "purchase_date": "date",
        "item_desc": "item_description",
        "item_no": "item_code",
        "price_usd": "price_paid",
    }
    df_final = df_final.rename(columns=rename_map)

    # Create a date column from separate values
    df_final["date"] = to_datetime(df_final["date"], format="%Y-%M-%d").dt.date

    # Remove prefix from customer ID
    df_final["customer_id"] = df_final["customer_id"].apply(lambda customer_id: customer_id.replace("C", ""))

    # Make purchase location lowercase
    df_final["purchase_location"] = "online"

    # Fill column with null values
    df_final["item_name"] = None

    # Fill in the currency value. Assume USD
    df_final["currency"] = "USD"

    # Get the columns and sort
    df_final = df_final.loc[
        :,
        [
            "customer_id",
            "date",
            "purchase_location",
            "item_code",
            "item_name",
            "item_description",
            "price_paid",
            "currency",
            # "address_state",
            # "address_postal_code",
            # "address_country",
        ],
    ]

    # Return dataframe
    return df_final
