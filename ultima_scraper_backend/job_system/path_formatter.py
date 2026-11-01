"""
Path formatting system for generating download paths and filenames.

This replaces the USC ReformatManager with a simpler, cleaner implementation
that works with the new Metadata models.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .metadata_models import MediaMetadata

__all__ = ["FormatAttributes", "PathFormatter", "invalid_format_tokens"]

# Older token names that saved formats may still use.
LEGACY_FORMAT_TOKENS = {
    "value": "content_value",
    "api_type": "content_type",
    "model_username": "user_username",
    "filename": "media_filename",
    "ext": "media_ext",
    "text": "content_text",
    "date": "content_date",
}
_FORMAT_TOKEN_PATTERN = re.compile(r"\{([^}]*)\}")


class FormatAttributes:
    site_name = "{site_name}"
    authed_username = "{authed_username}"
    user_id = "{user_id}"
    user_username = "{user_username}"
    first_username_letter = "{first_username_letter}"
    # Legacy download/metadata formats use {first_letter}; modern formats use
    # {first_username_letter}. Keep both names available during migration.
    first_letter = "{first_letter}"
    first_id_digit = "{first_id_digit}"
    content_id = "{content_id}"
    content_type = "{content_type}"
    content_value = "{content_value}"
    content_text = "{content_text}"
    content_date = "{content_date}"
    media_id = "{media_id}"
    media_type = "{media_type}"
    media_filename = "{media_filename}"
    media_ext = "{media_ext}"

    def whitelist(self, wl: list[str]) -> list[str]:
        new_wl: list[str] = []
        new_format_copied = copy.deepcopy(self)
        for _key, value in new_format_copied:
            if value not in wl:
                new_wl.append(value)
        return new_wl

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    @classmethod
    def get_all_keys(cls) -> list[str]:
        return [
            attr
            for attr in dir(cls)
            if not attr.startswith("_") and not callable(getattr(cls, attr))
        ]


class PathFormatter:
    """
    Formats file paths and names for downloaded media using template strings.

    Supports tokens like:
    - {site_name}: Site name (onlyfans, fansly, etc.)
    - {user_username}: Creator username ({model_username} is the legacy name)
    - {first_letter}: First letter of username (for organization)
    - {content_type}: Content type (Posts, Messages, Stories; legacy {api_type})
    - {content_value}: Paid/Free bucket (legacy {value})
    - {media_type}: Media category (images, videos, audio)
    - {media_id}: Media ID
    - {content_id}: Content/post ID
    - {filename}: Original filename (without extension)
    - {ext}: File extension
    - {text}: Content text (truncated)
    - {date}: Created date in various formats
    """

    def __init__(
        self,
        directory_format: str | Path,
        filename_format: str | Path,
        date_format: str = "%Y-%m-%d",
        text_length: int = 50,
    ):
        """
        Initialize path formatter.

        Args:
            directory_format: Directory path template (e.g., "{site_name}/{user_username}/{content_type}")
            filename_format: Filename template (e.g., "{filename}.{ext}")
            date_format: strftime format for {date} token
            text_length: Max length for {text} token
        """
        self.directory_format = directory_format
        self.filename_format = filename_format
        self.date_format = date_format
        self.text_length = text_length

    def _build_tokens(
        self,
        media: MediaMetadata,
        site_name: str,
        model_username: str,
        api_type: str,
        content_value: str | int,
        content_text: str | None,
        filename_stem: str,
        file_ext: str,
    ) -> dict[str, str]:
        """Build dictionary of all available tokens."""
        # Sanitize text
        safe_text = ""
        if content_text:
            safe_text = content_text[: self.text_length]
            # Remove newlines and invalid path characters
            safe_text = (
                safe_text.replace("\n", " ").replace("/", "-").replace("\\", "-")
            )
            safe_text = safe_text.strip()

        # Format date
        date_str = media.created_at.strftime(self.date_format)

        # First letter (for organizing by username first letter)
        first_letter = model_username[0].upper() if model_username else "0"

        return {
            "site_name": site_name,
            "model_username": model_username,
            "user_username": model_username,
            "first_letter": first_letter,
            "first_username_letter": first_letter,
            "api_type": api_type,
            "content_type": api_type,
            "value": str(content_value),
            "content_value": str(content_value),
            "media_type": media.media_type,
            "media_id": str(media.media_id),
            "content_id": str(media.content_id),
            "filename": filename_stem,
            "ext": file_ext,
            "text": safe_text,
            "date": date_str,
            "year": str(media.created_at.year),
            "month": f"{media.created_at.month:02d}",
            "day": f"{media.created_at.day:02d}",
        }

    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename."""
        # Characters not allowed in filenames (varies by OS, using most restrictive)
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, "_")

        # Remove control characters
        filename = "".join(c for c in filename if ord(c) >= 32)

        # Trim whitespace and dots from ends
        filename = filename.strip(". ")

        # Ensure filename is not empty
        if not filename:
            filename = "file"

        return filename

    @staticmethod
    def _is_archived_posts_type(value: str) -> bool:
        """Match archived-post content-type spellings used by older records."""
        normalized = re.sub(r"[\s_-]+", "", value).casefold()
        return normalized == "archivedposts"

    def get_user_directory_format(self) -> str | None:
        unique_tokens = {"{user_id}", "{user_username}", "{model_username}"}
        format_str = str(copy.deepcopy(self.directory_format))

        user_token_positions: list[tuple[int, str]] = []
        for token in unique_tokens:
            index = 0
            while True:
                index = format_str.find(token, index)
                if index == -1:
                    break
                user_token_positions.append((index, token))
                index += len(token)

        if not user_token_positions:
            return None

        user_token_positions.sort()
        current_end = user_token_positions[0][0] + len(user_token_positions[0][1])

        for pos, token in user_token_positions[1:]:
            between = format_str[current_end:pos].strip()
            if between in {"/", ""}:
                current_end = pos + len(token)
            else:
                break

        next_separator = format_str.find("/", current_end)
        if next_separator == -1:
            return format_str[:current_end]
        return format_str[:next_separator]

    def get_file_path_format(self) -> str:
        """Return the directory-format suffix after the performer directory."""
        user_dir_format = self.get_user_directory_format()
        if not user_dir_format:
            return str(self.directory_format)

        format_str = str(self.directory_format)
        user_dir_str = str(user_dir_format)
        if format_str.startswith(user_dir_str):
            remaining = format_str[len(user_dir_str) :]
            if remaining.startswith("/"):
                remaining = remaining[1:]
            return remaining
        return format_str

    def _format_legacy_path(
        self,
        format_attributes: FormatAttributes,
        unformatted_path: str,
        date_format: str,
        text_length: int,
    ) -> str:
        pattern = r"\{([^}]+)\}"
        matches = re.findall(pattern, unformatted_path)
        formatted_path = unformatted_path

        legacy_token_map = LEGACY_FORMAT_TOKENS
        capitalize = {
            "first_username_letter",
            "content_type",
            "media_type",
            "content_value",
        }
        custom = {"first_letter", "first_username_letter", "first_id_digit"}
        all_format_keys = set(FormatAttributes.get_all_keys())
        directory_format = str(self.directory_format)
        directory_has_content_type = any(
            token in directory_format for token in ("{api_type}", "{content_type}")
        )

        for match in matches:
            actual_token = legacy_token_map.get(match, match)
            if actual_token in custom:
                if actual_token in {"first_letter", "first_username_letter"}:
                    username = str(format_attributes.__dict__.get("user_username", ""))
                    value = username[0].upper() if username else "0"
                else:
                    user_id = str(format_attributes.__dict__.get("user_id", ""))
                    value = user_id[0] if user_id else "0"
            elif actual_token == "content_date":
                date_value = format_attributes.__dict__.get("content_date", "")
                value = (
                    date_value.strftime(date_format)
                    if hasattr(date_value, "strftime")
                    else str(date_value)
                )
            elif actual_token == "content_text":
                value = str(format_attributes.__dict__.get("content_text", ""))[
                    :text_length
                ]
            elif actual_token not in all_format_keys:
                raise ValueError(f"Unknown format attribute: {match}")
            else:
                try:
                    value = str(format_attributes.__dict__[actual_token])
                except KeyError as exc:
                    raise ValueError(f"Missing format value: {match}") from exc
                if (
                    actual_token == "content_type"
                    and directory_has_content_type
                    and self._is_archived_posts_type(value)
                ):
                    value = "Archived/Posts"
                elif actual_token in capitalize and not value.isdigit():
                    value = value.capitalize()

            formatted_path = formatted_path.replace(f"{{{match}}}", value)

        return formatted_path

    def _format_media_path(
        self,
        media: MediaMetadata,
        site_name: str,
        model_username: str,
        api_type: str,
        content_value: str | int,
        content_text: str | None = None,
        base_filename: str | None = None,
    ) -> tuple[Path, str]:
        if not base_filename:
            base_filename = media.extract_filename_from_url()

        filename_stem = Path(base_filename).stem or f"media_{media.media_id}"
        file_ext = Path(base_filename).suffix.lstrip(".") or "bin"

        tokens = self._build_tokens(
            media=media,
            site_name=site_name,
            model_username=model_username,
            api_type=api_type,
            content_value=content_value,
            content_text=content_text,
            filename_stem=filename_stem,
            file_ext=file_ext,
        )

        directory_tokens = dict(tokens)
        if self._is_archived_posts_type(api_type):
            # Keep the database token canonical while rendering the desired
            # two-level folder structure for downloaded files.
            directory_tokens["api_type"] = "Archived/Posts"
            directory_tokens["content_type"] = "Archived/Posts"

        try:
            directory_str = str(self.directory_format).format(**directory_tokens)
        except KeyError as e:
            raise ValueError(f"Missing token in directory_format: {e}") from e

        try:
            filename = str(self.filename_format).format(**tokens)
        except KeyError as e:
            raise ValueError(f"Missing token in filename_format: {e}") from e

        return Path(directory_str), self._sanitize_filename(filename)

    def format_path(self, *args: Any, **kwargs: Any) -> Any:
        if args and isinstance(args[0], FormatAttributes):
            format_attributes = args[0]
            unformatted_path = args[1] if len(args) > 1 else str(self.directory_format)
            date_format = args[2] if len(args) > 2 else self.date_format
            text_length = args[3] if len(args) > 3 else self.text_length
            return self._format_legacy_path(
                format_attributes,
                str(unformatted_path),
                str(date_format),
                int(text_length),
            )

        return self._format_media_path(*args, **kwargs)


def invalid_format_tokens(template: str) -> list[str]:
    """Return the {tokens} in a format template that the formatter cannot render."""
    valid = set(FormatAttributes.get_all_keys()) | set(LEGACY_FORMAT_TOKENS)
    invalid: list[str] = []
    for token in _FORMAT_TOKEN_PATTERN.findall(template):
        if token not in valid and token not in invalid:
            invalid.append(token)
    return invalid
