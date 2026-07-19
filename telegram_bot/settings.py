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
        """Generate human-readable settings description.
        
        Returns:
            Formatted settings string for display
        """
        choices = "\n".join(
            f"{'✅' if key == self.profile_key else '▫️'} {key}: {profile.label}"
            for key, profile in PROFILES.items()
        )
        doc_status = "ON" if self.send_as_document else "OFF"
        return (
            f"⚙️ <b>Encode Settings</b>\n\n"
            f"📊 <b>Current Profile:</b> {self.profile.name}\n"
            f"   {self.profile.resolution}p • CRF {self.profile.crf} • {CODEC_LABELS.get(self.profile.codec, self.profile.codec.upper())}\n\n"
            f"📄 <b>Send as Document:</b> {doc_status}\n\n"
            f"📋 <b>Available Profiles:</b>\n{choices}\n\n"
            f"💡 Use the buttons below to change settings."
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
        if self._load_from(self._path):
            return
        # Primary settings unreadable/corrupt — attempt to restore from backup
        backup_path = self._path.with_suffix(f"{self._path.suffix}.bak")
        if backup_path.exists() and self._load_from(backup_path):
            logging.getLogger(__name__).warning(
                "Restored settings from backup: %s", backup_path
            )

    def _load_from(self, path: Path) -> bool:
        """Load and validate settings from a path. Returns True on success."""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return False
        except (OSError, json.JSONDecodeError):
            logging.getLogger(__name__).warning("Ignoring unreadable settings file: %s", path)
            return False

        if not isinstance(payload, dict):
            return False

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
        return True

    def _save(self) -> None:
        import tempfile
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({
            "profiles": self._profiles,
            "document": self._doc_flags,
        }, indent=2, sort_keys=True) + "\n"
        
        # Create a backup of the previous settings just in case
        backup_path = self._path.with_suffix(f"{self._path.suffix}.bak")
        try:
            if self._path.exists():
                import shutil
                shutil.copy2(self._path, backup_path)
        except OSError:
            pass

        try:
            with tempfile.NamedTemporaryFile("w", dir=self._path.parent, delete=False, encoding="utf-8") as temp_file:
                temp_file.write(payload)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_path = Path(temp_file.name)
            os.replace(temp_path, self._path)
            try:
                os.chmod(self._path, 0o600)
            except OSError:
                pass
        except OSError as exc:
            try:
                if 'temp_path' in locals() and temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass
            raise RuntimeError("Could not save settings") from exc
