from pandera.pandas import DataFrameSchema, Column
# from datetime import datetime

schema = DataFrameSchema(
    columns={
        "person_id": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "address": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "city": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "state": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "zip_code": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
    },
    coerce=True,
    strict=False,
    ordered=False,
    report_duplicates="all",
    unique_column_names=False,
    add_missing_columns=False,
    name="Set2_Customer_Address_Input",
    title="Sample set 2 customer address input",
    description="Schema for input file for dataframe: Customer address",
)
