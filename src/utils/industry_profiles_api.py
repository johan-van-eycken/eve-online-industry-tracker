from __future__ import annotations

from utils.flask_api import cached_api_get


def fetch_industry_profiles(*, character_id: int) -> list[dict]:
    resp = cached_api_get(f"/industry_profiles/{int(character_id)}") or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load industry profiles")

    data = resp.get("data") or []
    return data if isinstance(data, list) else []


def build_industry_profile_options(
    profiles: list[dict],
) -> tuple[list[int], dict[int, str], int]:
    option_values: list[int] = [0]
    label_by_value: dict[int, str] = {0: "Default profile"}
    default_value = 0

    for profile in profiles:
        if not isinstance(profile, dict):
            continue

        raw_id = profile.get("id")
        if raw_id is None:
            continue
        try:
            profile_id = int(raw_id)
        except Exception:
            continue
        if profile_id <= 0:
            continue

        label = str(profile.get("profile_name") or profile_id)
        location_name = str(profile.get("location_name") or "").strip()
        if location_name:
            label = f"{label} ({location_name})"
        if bool(profile.get("is_default")):
            label = f"{label} [default]"
            default_value = profile_id
        elif default_value == 0:
            default_value = profile_id

        option_values.append(profile_id)
        label_by_value[profile_id] = label

    return option_values, label_by_value, default_value