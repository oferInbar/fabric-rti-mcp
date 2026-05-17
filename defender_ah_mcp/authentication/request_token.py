from contextvars import ContextVar

_request_token: ContextVar[str | None] = ContextVar("_request_token", default=None)


def set_auth_token(token: str | None) -> None:
    """Set the auth token for the current request context."""
    _request_token.set(token)


def get_auth_token() -> str | None:
    """Get the auth token from the current request context."""
    return _request_token.get()
