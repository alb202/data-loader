from pandera.pandas import DataFrameSchema, Column
# from datetime import datetime

customer2_schema = DataFrameSchema(
    columns={
        "cust_id": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "first_name": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "last_name": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "birth_year": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "sex": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "address_line": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "postal_code": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "country": Column(
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
    name="Set2_Customer_Input",
    title="Sample set 2 customer input",
    description="Schema for input file for dataframe: Customer",
)
