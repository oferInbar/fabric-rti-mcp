from __future__ import annotations

import re

_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
_TRAILING_HEX_RE = re.compile(r"_[0-9a-f]{12,}$", re.IGNORECASE)
_MAX_LABEL_LEN = 60


def _strip_noisy_suffix(name: str) -> str:
    name = _UUID_RE.sub("", name)
    name = _TRAILING_HEX_RE.sub("", name)
    name = name.rstrip("_-")
    if len(name) > _MAX_LABEL_LEN:
        name = name[: _MAX_LABEL_LEN - 1] + "…"
    return name


def friendly_user_name(arn: str | None) -> str:
    """Compact, human-readable label for an AWS principal ARN.

    Examples:
      arn:aws:iam::123:user/alice                                 -> alice
      arn:aws:sts::123:assumed-role/AdminRole/alice@contoso.com   -> alice@contoso.com
      arn:aws:sts::123:assumed-role/EKSnoderole/i-0a6656eb...     -> EKSnoderole · i-0a6656eb...
      arn:aws:sts::123:assumed-role/MDCRole/MDC_<uuid>            -> MDCRole · MDC
    """
    if not arn:
        return "(unknown)"
    s = str(arn)
    if s.startswith("arn:aws:iam::") and ":user/" in s:
        return s.rsplit("/", 1)[-1]
    if ":assumed-role/" in s:
        tail = s.split(":assumed-role/", 1)[1]
        parts = tail.split("/", 1)
        role = parts[0]
        session = parts[1] if len(parts) > 1 else ""
        if "@" in session:
            return session
        session_short = _strip_noisy_suffix(session)
        if not session_short or session_short == role:
            return role
        return f"{role} · {session_short}"
    if "/" in s:
        return s.rsplit("/", 1)[-1]
    return s
