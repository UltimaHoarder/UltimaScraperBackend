"""Small shared preview projection; media endpoints own storage resolution."""


def media_preview(site: str, media_id: int, category: str | None, preview: bool, duration: float | None) -> dict:
    thumbnail = f"/api/v1/media/thumbnail/{site}/{media_id}"
    return {"id": media_id, "category": category or "image", "preview": preview,
            "duration": duration, "thumb_url": f"{thumbnail}?local_only=true",
            "fallback_thumb_url": f"{thumbnail}?remote_only=true",
            "local_url": f"/api/v1/media/stream/{site}/{media_id}"}
