from typing import Annotated

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
)


class AccountConfig(BaseModel):
    """Validates user-specific credentials."""

    username: Annotated[str | None, Field(description="Username or Email")] = None
    password: Annotated[SecretStr | None, Field(description="Password")] = None
    token: Annotated[SecretStr | None, Field(description="Socrata Token")] = None
