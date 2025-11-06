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
        "full_name": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "age": Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            required=True,
        ),
        "gender": Column(
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
    name="Set2_Customer_Details_Input",
    title="Sample set 2 customer details input",
    description="Schema for input file for dataframe: Customer details",
)
