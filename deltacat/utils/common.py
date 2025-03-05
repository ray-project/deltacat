import hashlib
import os
import time
from typing import Any, Dict


def env_bool(key: str, default: bool) -> int:
    if key in os.environ:
        return bool(os.environ[key])
    return default


def env_integer(key: str, default: int) -> int:
    if key in os.environ:
        return int(os.environ[key])
    return default


def env_string(key: str, default: str) -> str:
    if key in os.environ:
        return os.environ[key]
    return default


def current_time_ms() -> int:
    return int(round(time.time() * 1000))


def sha1_digest(_bytes) -> bytes:
    hasher = hashlib.sha1()
    hasher.update(_bytes)
    return hasher.digest()


def sha1_hexdigest(_bytes) -> str:
    hasher = hashlib.sha1()
    hasher.update(_bytes)
    return hasher.hexdigest()


class ContentTypeKwargsProvider:
    """Abstract callable that takes a content type and keyword arg dictionary
    as input, and returns finalized keyword args as output. Useful for merging
    content-type-specific keyword arguments into an existing fixed dictionary
    of keyword arguments."""

    def _get_kwargs(self, content_type: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def __call__(self, content_type: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._get_kwargs(content_type, kwargs)


ReadKwargsProvider = ContentTypeKwargsProvider
