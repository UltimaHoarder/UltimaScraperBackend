"""Shared creator and availability filters for favourite pages and counts."""
from typing import Annotated, Literal
from fastapi import HTTPException, Query

Availability = Literal["available", "inactive", "deleted", "expired", "unavailable"]
TriState = Literal["none", "include", "exclude"]
Identity = Annotated[list[int] | None, Query(max_length=8)]
Number = Annotated[float | None, Query(ge=0, allow_inf_nan=False)]
Count = Annotated[int | None, Query(ge=0, le=2**63-1)]


def favourite_filters(
    availability: Availability | None = None,
    active_only: bool = False, performer: TriState = "none", subscribed: TriState = "none",
    sex: Identity = None, gender: Identity = None,
    profile_type: Identity = None, sexual_content: Identity = None,
    min_price: Number = None, max_price: Number = None,
    min_post_count: Count = None, max_post_count: Count = None,
    min_image_count: Count = None, max_image_count: Count = None,
    min_video_count: Count = None, max_video_count: Count = None,
) -> dict:
    values = locals().copy()
    for name, maximum in (("sex", 2), ("gender", 2), ("profile_type", 3), ("sexual_content", 6)):
        if values[name] and any(value < -1 or value > maximum for value in values[name]):
            raise HTTPException(422, f"Invalid {name} filter")
        if values[name]: values[name] = sorted(set(values[name]))
    for name in ("price", "post_count", "image_count", "video_count"):
        lower, upper = values[f"min_{name}"], values[f"max_{name}"]
        if lower is not None and upper is not None and lower > upper:
            raise HTTPException(422, f"Minimum {name} exceeds maximum")
    return {key: value for key, value in values.items() if value is not None and value != [] and value != "none" and value is not False}
