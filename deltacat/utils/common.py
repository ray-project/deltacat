import argparse
import hashlib
import os
import time
import sys
from typing import Any, Dict


def get_cli_arg(key: str) -> Any:
    parser = argparse.ArgumentParser()

    # allow hyphens in keys, normalize as underscores
    normalized_key = key.replace("-", "_")

    parser.add_argument(f"--{key}", metavar=normalized_key, type=str)
    args, _ = parser.parse_known_args(sys.argv[1:])  # Allow unknown args
    return getattr(args, normalized_key, None)


def env_bool(key: str, default: bool) -> bool:
    cli_value = get_cli_arg(key)
    if cli_value is not None:
        return bool(cli_value)

    if key in os.environ:
        return bool(os.environ[key])

    return default


def env_integer(key: str, default: int) -> int:
    cli_value = get_cli_arg(key)
    if cli_value is not None:
        return int(cli_value)

    if key in os.environ:
        return int(os.environ[key])

    return default


def env_string(key: str, default: str) -> str:
    return get_cli_arg(key) or os.getenv(key, default)


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
