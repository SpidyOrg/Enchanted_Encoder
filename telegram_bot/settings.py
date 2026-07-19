from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path


CODEC_LABELS = {
    "h264": "H.264",
    "hevc": "H.265/HEVC",
    "av1": "AV1",
}

CODEC_ENCODERS = {
    "h264": "libx264",
    "hevc": "libx265",
    "av1": "libsvtav1",
}


@dataclass(frozen=True)
class EncodeProfile:
    key: str
    name: str
    resolution: int
    crf: int
    preset: str
    audio_bitrate: str
    codec: str = "h264"

    @property
    def label(self) -> str:
        codec_str = CODEC_LABELS.get(self.codec, self.codec.upper())
        return (
            f"{self.name} - {self.resolution}p CRF {self.crf}, "
            f"{codec_str}/{self.preset}, AAC {self.audio_bitrate}"
        )

    @property
    def encoder_name(self) -> str:
        return CODEC_ENCODERS.get(self.codec, "libx264")


PROFILES: dict[str, EncodeProfile] = {
    "balanced": EncodeProfile(
        key="balanced", name="Balanced", resolution=720, crf=22,
        preset="veryfast", audio_bitrate="128k", codec="h264",
    ),
    "speed": EncodeProfile(
        key="speed", name="Speed", resolution=540, crf=24,
        preset="superfast", audio_bitrate="96k", codec="h264",
    ),
    "compact": EncodeProfile(
        key="compact", name="Compact", resolution=480, crf=26,
        preset="veryfast", audio_bitrate="96k", codec="h264",
    ),
    "quality": EncodeProfile(
        key="quality", name="Quality", resolution=720, crf=20,
        preset="faster", audio_bitrate="160k", codec="h264",
    ),
    "tiny": EncodeProfile(
        key="tiny", name="Tiny", resolution=360, crf=28,
        preset="superfast", audio_bitrate="64k", codec="h264",
    ),
    "hevc": EncodeProfile(
        key="hevc", name="HEVC", resolution=720, crf=26,
        preset="medium", audio_bitrate="96k", codec="hevc",
    ),
    "av1": EncodeProfile(
        key="av1", name="AV1", resolution=720, crf=30,
        preset="10", audio_bitrate="96k", codec="av1",
    ),
}
DEFAULT_PROFILE = "balanced"


@dataclass
class EncodeSettings:
    profile_key: str = DEFAULT_PROFILE
    send_as_document: bool = False

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
        doc_status = "on" if self.send_as_document else "off"
        footer = (
            "Use /settings <profile> to change encode profile.\n"
            "Profiles: balanced, speed, compact, quality, tiny, hevc, av1.\n"
            "Use /settings document on|off to toggle send as file."
        )
        return (
            "🎛 Encode settings\n"
            f"Current: {self.profile.label}\n"
            f"Send as document: {doc_status}\n\n"
            f"{choices}\n\n"
            f"{footer}"
        )


class SettingsStore:
    """Small durable per-user profile store with atomic JSON writes."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._profiles: dict[str, str] = {}
        self._doc_flags: dict[str, bool] = {}
        self._load()

    def get(self, user_id: str) -> EncodeSettings:
        uid = str(user_id)
        profile_key = self._profiles.get(uid, DEFAULT_PROFILE)
        if profile_key not in PROFILES:
            profile_key = DEFAULT_PROFILE
        return EncodeSettings(
            profile_key=profile_key,
            send_as_document=self._doc_flags.get(uid, False),
        )

    def set_profile(self, user_id: str, profile_key: str) -> EncodeSettings | None:
        settings = self.get(user_id)
        if not settings.set_profile(profile_key):
            return None
        self._profiles[str(user_id)] = settings.profile_key
        self._save()
        return settings

    def set_document(self, user_id: str, value: bool) -> EncodeSettings:
        uid = str(user_id)
        self._doc_flags[uid] = value
        self._save()
        return self.get(user_id)

    def _load(self) -> None:
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            profiles = payload.get("profiles", {})
            if isinstance(profiles, dict):
                self._profiles = {
                    str(uid): profile
                    for uid, profile in profiles.items()
                    if profile in PROFILES
                }
            doc_flags_raw = payload.get("document", {})
            if isinstance(doc_flags_raw, dict):
                self._doc_flags = {
                    str(uid): bool(val)
                    for uid, val in doc_flags_raw.items()
                }
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError, AttributeError):
            logging.getLogger(__name__).warning("Ignoring unreadable settings file: %s", self._path)

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        payload = json.dumps({
            "profiles": self._profiles,
            "document": self._doc_flags,
        }, indent=2, sort_keys=True) + "\n"
        try:
            temporary.write_text(payload, encoding="utf-8")
            os.replace(temporary, self._path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise RuntimeError("Could not save settings") from exc
