from pandas import Timestamp
from pandera.pandas import DataFrameSchema, Column, Check
from datetime import datetime

check_currencies = ["USD", "EUR"]
check_purchase_location = ["online", "store"]


schema = DataFrameSchema(
    columns={
        "customer_id": Column(
            dtype=str,
            nullable=False,
            unique=False,
            coerce=True,
            required=True,
        ),
        "date": Column(
            dtype="datetime64[ns]",
            checks=[
                Check.greater_than_or_equal_to(
                    min_value=Timestamp("2000-01-01 00:00:00"),
                    raise_warning=False,
                    ignore_na=True,
                ),
                Check.less_than_or_equal_to(
                    max_value=datetime.now(),
                    raise_warning=False,
                    ignore_na=True,
                ),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "purchase_location": Column(
            dtype="object",
            checks=Check.isin(check_purchase_location),
            nullable=False,
            unique=False,
            coerce=True,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "item_code": Column(
            dtype="object",
            checks=None,
            nullable=False,
            unique=False,
            coerce=True,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "item_name": Column(
            dtype="object",
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
            regex=False,
        ),
        "item_description": Column(
            dtype="object",
            nullable=False,
            unique=False,
            coerce=True,
            required=True,
            regex=False,
        ),
        "price_paid": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=0, raise_warning=True, ignore_na=False),
                Check.less_than_or_equal_to(max_value=1_000_000, raise_warning=True, ignore_na=False),
            ],
            nullable=False,
            unique=False,
            coerce=True,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "currency": Column(
            dtype="object",
            checks=Check.isin(check_currencies),
            nullable=False,
            unique=False,
            coerce=True,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
    },
    coerce=True,
    strict=True,
    ordered=True,
    report_duplicates="all",
    unique_column_names=True,
    add_missing_columns=True,
    name="Purchases_model",
    title="Purchases Model",
    description="Schema for purchases data model",
)
