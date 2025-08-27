from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from typing import Any


def get_dist_version(dist_name: str, module: Any | None = None, default: str = "0") -> str:
    try:
        return pkg_version(dist_name)
    except PackageNotFoundError:
        if module is not None:
            return getattr(module, "__version__", default)
        return default


def parse_major(v: str) -> int:
    nums = "".join(ch if ch.isdigit() or ch == "." else "." for ch in v).split(".")
    try:
        return int(nums[0] or 0)
    except ValueError:
        return 0


def require_major_at_least(
    dist_name: str, module: Any | None, required_major: int, feature_name: str
) -> None:
    v = get_dist_version(dist_name, module=module, default="0")
    if parse_major(v) < required_major:
        raise ImportError(
            f"{feature_name} requires {dist_name}>={required_major}.0, but found {v!r}."
        )
