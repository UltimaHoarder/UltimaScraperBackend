def normalize_age_verification_state(
    is_age_verified: bool | None,
    age_verification_required: bool | None,
) -> tuple[bool | None, bool | None]:
    """Keep OnlyFans age-verification flags internally consistent."""
    if is_age_verified is True:
        return True, False
    if age_verification_required is True:
        return False, True
    return is_age_verified, age_verification_required
