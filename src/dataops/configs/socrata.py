from typing import Annotated

from pydantic import (
    BaseModel,
    Field,
    # SecretStr,
    field_validator,
)


class SocrataTableID(BaseModel):
    """Socrata Platform Unique Table Identifier."""

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


class SocrataAPIConfig(BaseModel):
    """Validates API-specific details."""

    domain: str = Field(
        default="data.ct.gov",
        description="Domain name for data portal platform.",
    )
    source: SocrataTableID
    target: SocrataTableID | None = None
