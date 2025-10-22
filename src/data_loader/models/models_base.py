import pandera.pandas as pa
# from typing import Type


class BaseModel(pa.DataFrameModel):
    """Base Pandera DataFrameModel for shared behavior."""

    @classmethod
    def validate_df(cls, df, lazy=True):
        """Validate a dataframe and raise detailed errors if invalid."""
        try:
            return cls.validate(df, lazy=lazy)
        except pa.errors.SchemaErrors as err:
            print(" Validation errors:")
            print(err.failure_cases)
            raise
