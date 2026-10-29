from __future__ import annotations

import copy
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import ultima_scraper_api

    user_types = ultima_scraper_api.user_types
    auth_types = ultima_scraper_api.auth_types


class FormatAttributes(object):
    def __init__(self):
        self.site_name = "{site_name}"
        self.first_letter = "{first_letter}"
        self.content_id = "{content_id}"
        self.media_id = "{media_id}"
        self.profile_username = "{profile_username}"
        self.model_username = "{model_username}"
        self.api_type = "{api_type}"
        self.media_type = "{media_type}"
        self.filename = "{filename}"
        self.value = "{value}"
        self.text = "{text}"
        self.date = "{date}"
        self.ext = "{ext}"

    def whitelist(self, wl: list[str]):
        new_wl: list[str] = []
        new_format_copied = copy.deepcopy(self)
        for _key, value in new_format_copied:
            if value not in wl:
                new_wl.append(value)
        return new_wl

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
