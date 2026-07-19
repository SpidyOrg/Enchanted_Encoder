from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path


@dataclass(frozen=True)
class EncodeProfile:
    key: str
    name: str
    resolution: int
    crf: int
    x264_preset: str
    audio_bitrate: str

    @property
    def label(self) -> str:
        return (
            f"{self.name} - {self.resolution}p CRF {self.crf}, "
            f"H.264/{self.x264_preset}, AAC {self.audio_bitrate}"
        )


PROFILES: dict[str, EncodeProfile] = {
    "balanced": EncodeProfile(
        key="balanced",
        name="Balanced",
        resolution=720,
        crf=22,
        x264_preset="veryfast",
        audio_bitrate="128k",
    ),
    "speed": EncodeProfile(
        key="speed",
        name="Speed",
        resolution=540,
        crf=24,
        x264_preset="superfast",
        audio_bitrate="96k",
    ),
    "compact": EncodeProfile(
        key="compact",
        name="Compact",
        resolution=480,
        crf=26,
        x264_preset="veryfast",
        audio_bitrate="96k",
    ),
    "quality": EncodeProfile(
        key="quality",
        name="Quality",
        resolution=720,
        crf=20,
        x264_preset="faster",
        audio_bitrate="160k",
    ),
    "tiny": EncodeProfile(
        key="tiny",
        name="Tiny",
        resolution=360,
        crf=28,
        x264_preset="superfast",
        audio_bitrate="64k",
    ),
}
DEFAULT_PROFILE = "balanced"


@dataclass
class EncodeSettings:
    profile_key: str = DEFAULT_PROFILE

    @property
    def profile(self) -> EncodeProfile:
        return PROFILES[self.profile_key]

    def set_profile(self, profile_key: str) -> bool:
        normalized = profile_key.strip().lower()
        if normalized not in PROFILES:
            return False
        self.profile_key = normalized
        return True

    def describe(self) -> str:
        choices = "\n".join(
            f"{'✅' if key == self.profile_key else '▫️'} {key}: {profile.label}"
            for key, profile in PROFILES.items()
        )
        return (
            "🎛 Encode settings\n"
            f"Current: {self.profile.label}\n\n"
            f"{choices}\n\n"
            "Use /settings <profile>: balanced, speed, compact, quality, or tiny."
        )


class SettingsStore:
    """Small durable per-user profile store with atomic JSON writes."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._profiles: dict[str, str] = {}
        self._load()

    def get(self, user_id: str) -> EncodeSettings:
        profile_key = self._profiles.get(str(user_id), DEFAULT_PROFILE)
        if profile_key not in PROFILES:
            profile_key = DEFAULT_PROFILE
        return EncodeSettings(profile_key)

    def set_profile(self, user_id: str, profile_key: str) -> EncodeSettings | None:
        settings = self.get(user_id)
        if not settings.set_profile(profile_key):
            return None
        self._profiles[str(user_id)] = settings.profile_key
        self._save()
        return settings

    def _load(self) -> None:
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            profiles = payload.get("profiles", {})
            if isinstance(profiles, dict):
                self._profiles = {
                    str(user_id): profile
                    for user_id, profile in profiles.items()
                    if profile in PROFILES
                }
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError, AttributeError):
            logging.getLogger(__name__).warning("Ignoring unreadable settings file: %s", self._path)

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        payload = json.dumps({"profiles": self._profiles}, indent=2, sort_keys=True) + "\n"
        try:
            temporary.write_text(payload, encoding="utf-8")
            os.replace(temporary, self._path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise RuntimeError("Could not save settings") from exc
