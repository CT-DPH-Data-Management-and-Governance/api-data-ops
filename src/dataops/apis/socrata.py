from typing import Annotated

from pydantic import (
    BaseModel,
    Field,
    field_validator,
)


class Table(BaseModel):
    """Socrata Platform Unique Table Identifier."""

    # domain
    id: Annotated[
        str,
        Field(description="Table ID (aka 'four by four') of source data table."),
    ]

    @field_validator("id")
    def _check_id(cls, v: str) -> str:
        overall = len(v) == 9
        v_parts = str.split(v, "-")
        fourbyfour = len(v_parts[0]) == len(v_parts[1])
        if not overall & fourbyfour:
            raise ValueError(f"{v} is not a valid socrata table id")
        return v
