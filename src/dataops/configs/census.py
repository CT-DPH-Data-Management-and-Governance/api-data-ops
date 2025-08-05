from typing import Annotated

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    # field_validator,
)


class CensusConfig(BaseModel):
    """Validate Census API specific details."""

    token: Annotated[SecretStr | None, Field(description="Census API Token")] = None
