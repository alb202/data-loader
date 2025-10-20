import pandera.pandas as pa
from pandera.typing import Series
from data_loader.models.models_base import BaseModel


class FinancialInputModel(BaseModel):
    """Schema for input financial data."""

    symbol: Series[str] = pa.Field(nullable=False)
    Date: Series[pa.DateTime] = pa.Field(nullable=False)
    Open: Series[float]
    High: Series[float]
    Low: Series[float]
    Close: Series[float]
    Volume: Series[int] = pa.Field(ge=0)


class FinancialOutputModel(BaseModel):
    """Schema for transformed and cleaned financial data."""

    symbol: Series[str]
    date: Series[pa.DateTime]
    open: Series[float]
    high: Series[float]
    low: Series[float]
    close: Series[float]
    volume: Series[int]
    returns: Series[float] = pa.Field(nullable=True)
