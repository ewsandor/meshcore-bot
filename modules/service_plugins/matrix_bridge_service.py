#!/usr/bin/env python3
"""Matrix bridge service for MeshCore Bot.

Bridges configured MeshCore channel messages to Matrix rooms and, optionally,
Matrix text messages back to configured MeshCore channels. Matrix-specific
client code is intentionally isolated in this service module.
"""

import asyncio
import contextlib
import copy
import json
import os
import re
import secrets
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from meshcore import EventType

try:
    from nio import (
        AsyncClient,
        AsyncClientConfig,
        KeyVerificationAccept,
        KeyVerificationCancel,
        KeyVerificationEvent,
        KeyVerificationKey,
        KeyVerificationMac,
        RoomMessageText,
        ToDeviceError,
    )
    MATRIX_NIO_AVAILABLE = True
except ImportError:
    AsyncClient = None  # type: ignore[assignment,misc]
    AsyncClientConfig = None  # type: ignore[assignment,misc]
    KeyVerificationAccept = object  # type: ignore[assignment,misc]
    KeyVerificationCancel = object  # type: ignore[assignment,misc]
    KeyVerificationEvent = object  # type: ignore[assignment,misc]
    KeyVerificationKey = object  # type: ignore[assignment,misc]
    KeyVerificationMac = object  # type: ignore[assignment,misc]
    RoomMessageText = object  # type: ignore[assignment,misc]
    ToDeviceError = object  # type: ignore[assignment,misc]
    MATRIX_NIO_AVAILABLE = False

from ..profanity_filter import censor, contains_profanity
from ..security_utils import validate_pubkey_format
from .base_service import BaseServicePlugin

MATRIX_MAX_MESSAGE_LENGTH = 4096
MESHCORE_CHANNEL_MESSAGE_MAX_BYTES = 133


@dataclass
class QueuedMessage:
    """A Matrix room message waiting to be sent."""

    room_id: str
    body: str
    channel_name: str
    retry_count: int = 0
    first_queued: float = 0.0
    next_retry_at: float = 0.0

    def __post_init__(self) -> None:
        now = time.time()
        if self.first_queued == 0.0:
            self.first_queued = now
        if self.next_retry_at == 0.0:
            self.next_retry_at = now


@dataclass
class PendingVerification:
    """Temporary state for one Matrix-to-MeshCore verification attempt."""

    matrix_user_id: str
    matrix_room_id: str
    meshcore_public_key: str
    challenge: str
    created_at: float
    expires_at: float
    stage: str = "mesh_challenge"
    transaction_id: Optional[str] = None
    matrix_device_id: Optional[str] = None


class MatrixBridgeService(BaseServicePlugin):
    """Bridge MeshCore channels to Matrix rooms, with optional inbound routing."""

    config_section = "MatrixBridge"
    description = "Bridges MeshCore channel messages to Matrix rooms"

    settings_schema = [
        {"key": "homeserver", "label": "Homeserver URL", "type": "str", "default": "",
         "help": "Matrix homeserver URL, for example https://matrix.example.org."},
        {"key": "user_id", "label": "Matrix user ID", "type": "str", "default": "",
         "help": "Full Matrix user ID, for example @meshbot:example.org."},
        {"key": "access_token", "label": "Access token", "type": "str", "default": "",
         "help": "Access token for the Matrix bot. Keep this secret."},
        {"key": "device_id", "label": "Device ID", "type": "str", "default": "MESHCOREBOT",
         "help": "Stable device ID used for the access token and encrypted rooms."},
        {"key": "store_path", "label": "Crypto store path", "type": "str", "default": "data/matrix-store",
         "help": "Persistent matrix-nio store for stable encrypted-room sessions."},
        {"key": "encryption_enabled", "label": "Enable E2E encryption", "type": "bool", "default": True,
         "help": "Enable matrix-nio encryption support and persist encryption state."},
        {"key": "max_message_length", "label": "Max message length", "type": "int",
         "min": 1, "max": MATRIX_MAX_MESSAGE_LENGTH, "default": MATRIX_MAX_MESSAGE_LENGTH,
         "help": "Maximum body length sent to Matrix or MeshCore."},
        {"key": "filter_profanity", "label": "Profanity filter", "type": "enum",
         "options": [{"value": "drop", "label": "Drop (don't bridge)"},
                     {"value": "censor", "label": "Censor (****)"},
                     {"value": "off", "label": "Off"}],
         "default": "drop", "help": "How to handle profanity in messages and usernames."},
        {"key": "bridge_bot_responses", "label": "Bridge bot responses", "type": "bool",
         "default": True, "help": "Also bridge the bot's own channel replies."},
        {"key": "verification_enabled", "label": "Enable identity verification", "type": "bool",
         "default": True, "help": "Allow Matrix users to link and verify a MeshCore identity."},
        {"key": "verification_timeout_seconds", "label": "Verification timeout", "type": "int",
         "min": 30, "max": 3600, "default": 300,
         "help": "Seconds before a pending identity or SAS verification expires."},
    ]
    settings_dynamic_sections = [
        {"section": "MatrixBridge", "key_prefix": "bridge.",
         "label": "Channel mappings", "key_label": "MeshCore channel", "value_label": "Matrix room ID",
         "help": "Map a MeshCore channel to a Matrix room ID. DMs are never bridged.",
         "key_placeholder": "Public", "value_placeholder": "!room:example.org"},
        {"section": "MatrixBridge", "key_prefix": "inbound.",
         "label": "Inbound channel mappings", "key_label": "MeshCore channel", "value_label": "Enabled",
         "help": "Set true to relay text from the mapped Matrix room back to this MeshCore channel.",
         "key_placeholder": "Public", "value_placeholder": "true"},
    ]

    def __init__(self, bot: Any):
        super().__init__(bot)
        config = self.bot.config
        self.enabled = config.getboolean(self.config_section, "enabled", fallback=False)
        self.homeserver = config.get(self.config_section, "homeserver", fallback="").strip().rstrip("/")
        self.user_id = config.get(self.config_section, "user_id", fallback="").strip()
        self.access_token = (
            os.environ.get("MATRIX_ACCESS_TOKEN") or config.get(self.config_section, "access_token", fallback="")
        ).strip()
        self.device_id = config.get(self.config_section, "device_id", fallback="MESHCOREBOT").strip()
        self.store_path = config.get(self.config_section, "store_path", fallback="data/matrix-store").strip()
        self.encryption_enabled = config.getboolean(self.config_section, "encryption_enabled", fallback=True)
        self.max_message_length = min(
            max(1, config.getint(self.config_section, "max_message_length", fallback=MATRIX_MAX_MESSAGE_LENGTH)),
            MATRIX_MAX_MESSAGE_LENGTH,
        )
        raw_filter = config.get(self.config_section, "filter_profanity", fallback="drop").strip().lower()
        self.filter_profanity = raw_filter if raw_filter in ("drop", "censor", "off") else "drop"
        self.bridge_bot_responses = config.getboolean(self.config_section, "bridge_bot_responses", fallback=True)
        self.verification_enabled = config.getboolean(self.config_section, "verification_enabled", fallback=True)
        self.verification_timeout_seconds = min(
            max(30, config.getint(self.config_section, "verification_timeout_seconds", fallback=300)),
            3600,
        )
        self.channel_rooms: dict[str, str] = {}
        self.inbound_channels: set[str] = set()
        self._load_channel_mappings()

        self.client: Optional[Any] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._queue_processor_task: Optional[asyncio.Task] = None
        self.message_queues: dict[str, list[QueuedMessage]] = {}
        self.send_times: dict[str, deque] = {}
        self.rate_limit_min_interval = 1.0
        self.max_retries = 5
        self.retry_delay_base = 1.0
        self.max_queue_age = 300
        self.pending_verifications: dict[str, PendingVerification] = {}
        self._linked_meshcore_keys: dict[str, str] = {}
        self._verification_tasks: dict[str, asyncio.Task] = {}
        self._verification_state_path = Path(self.store_path) / "meshcore-links.json"
        self._load_linked_identities()

        if not MATRIX_NIO_AVAILABLE:
            self.logger.error("matrix-nio is not installed. Matrix bridge is disabled.")
            self.enabled = False
        elif not self.homeserver or not self.user_id or not self.access_token:
            self.logger.error("Matrix bridge requires homeserver, user_id, and access_token.")
            self.enabled = False
        elif self.verification_enabled and not self.encryption_enabled:
            self.logger.error("Matrix identity verification requires encryption_enabled=true.")
            self.verification_enabled = False

    def _load_channel_mappings(self) -> None:
        if not self.bot.config.has_section(self.config_section):
            return
        for key, value in self.bot.config.items(self.config_section):
            if key.startswith("bridge.") and value.strip():
                self.channel_rooms[key[7:].strip()] = value.strip()
            elif key.startswith("inbound.") and self.bot.config.getboolean(
                self.config_section, key, fallback=False
            ):
                self.inbound_channels.add(key[8:].strip().lstrip("#").lower())
        for room_id in self.channel_rooms.values():
            self.message_queues.setdefault(room_id, [])
            self.send_times.setdefault(room_id, deque())

    def _load_linked_identities(self) -> None:
        try:
            with self._verification_state_path.open(encoding="utf-8") as state_file:
                loaded = json.load(state_file)
            if isinstance(loaded, dict):
                self._linked_meshcore_keys = {
                    str(matrix_user): str(mesh_key).lower()
                    for matrix_user, mesh_key in loaded.items()
                    if validate_pubkey_format(str(mesh_key))
                }
        except (FileNotFoundError, OSError, ValueError, TypeError):
            self._linked_meshcore_keys = {}

    def _save_linked_identities(self) -> None:
        try:
            self._verification_state_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = self._verification_state_path.with_suffix(".tmp")
            with temporary_path.open("w", encoding="utf-8") as state_file:
                json.dump(self._linked_meshcore_keys, state_file, indent=2, sort_keys=True)
            temporary_path.replace(self._verification_state_path)
        except OSError as exc:
            self.logger.error("Could not save Matrix/MeshCore identity links: %s", exc)

    def _find_pending_by_mesh_key(self, public_key: str) -> Optional[PendingVerification]:
        normalized = public_key.lower()
        return next(
            (pending for pending in self.pending_verifications.values()
             if pending.meshcore_public_key.lower() == normalized),
            None,
        )

    def _find_linked_user_by_mesh_key(self, public_key: str) -> Optional[str]:
        normalized = public_key.lower()
        return next(
            (matrix_user for matrix_user, mesh_key in self._linked_meshcore_keys.items()
             if mesh_key.lower() == normalized),
            None,
        )

    def _resolve_meshcore_public_key(self, prefix: str) -> Optional[str]:
        normalized = prefix.strip().lower()
        if validate_pubkey_format(normalized):
            return normalized
        contacts = getattr(self.bot.meshcore, "contacts", {}) or {}
        for contact in contacts.values():
            public_key = str(contact.get("public_key", "")).lower()
            if public_key and public_key.startswith(normalized) and validate_pubkey_format(public_key):
                return public_key
        return None

    async def _send_matrix_status(self, room_id: str, text: str) -> None:
        await self._queue_message(room_id, f"[Matrix verification] {text}", "verification")

    def _clear_pending_verification(self, matrix_user_id: str) -> None:
        self.pending_verifications.pop(matrix_user_id, None)
        task = self._verification_tasks.pop(matrix_user_id, None)
        if task and task is not asyncio.current_task():
            task.cancel()

    async def _handle_matrix_control_message(self, room: Any, event: Any) -> bool:
        if not self.verification_enabled or event.sender == self.user_id:
            return False
        members = getattr(room, "members", None)
        if members is not None and len(members) > 2:
            return False
        content = event.body.strip()
        if content.lower() in ("help", "matrix help", "link help"):
            await self._send_matrix_status(
                room.room_id,
                "Commands:\n"
                "link meshcore <64-character public key> - link and verify your MeshCore identity\n"
                "link status - show the current link\n"
                "unlink meshcore - remove the current link",
            )
            return True
        if content.lower() == "link status":
            linked_key = self._linked_meshcore_keys.get(event.sender)
            status = (
                f"Linked to MeshCore identity {linked_key[:12]}..."
                if linked_key else "No MeshCore identity is linked to this Matrix account."
            )
            await self._send_matrix_status(room.room_id, status)
            return True
        if content.lower() == "unlink meshcore":
            if self._linked_meshcore_keys.pop(event.sender, None) is not None:
                self._save_linked_identities()
                status = "Your MeshCore identity link has been removed."
            else:
                status = "No MeshCore identity was linked to this Matrix account."
            await self._send_matrix_status(room.room_id, status)
            return True
        match = re.fullmatch(r"link\s+meshcore\s+([0-9a-fA-F]{64})", content, re.IGNORECASE)
        if not match:
            return False

        public_key = match.group(1).lower()
        existing_key = self._linked_meshcore_keys.get(event.sender)
        if existing_key and existing_key != public_key:
            await self._send_matrix_status(
                room.room_id,
                "This Matrix account is already linked to another MeshCore identity. "
                "Use `unlink meshcore` through an administrator before changing it.",
            )
            return True
        if self._find_pending_by_mesh_key(public_key):
            await self._send_matrix_status(room.room_id, "A verification request for that MeshCore identity is already pending.")
            return True
        if event.sender in self.pending_verifications:
            await self._send_matrix_status(room.room_id, "A verification request for this Matrix account is already pending.")
            return True
        linked_user = self._find_linked_user_by_mesh_key(public_key)
        if linked_user and linked_user != event.sender:
            await self._send_matrix_status(room.room_id, "That MeshCore identity is already linked to another Matrix account.")
            return True

        now = time.time()
        challenge = secrets.token_hex(4).upper()
        pending = PendingVerification(
            matrix_user_id=event.sender,
            matrix_room_id=room.room_id,
            meshcore_public_key=public_key,
            challenge=challenge,
            created_at=now,
            expires_at=now + self.verification_timeout_seconds,
            matrix_device_id=(event.source.get("unsigned", {}).get("device_id")
                              if isinstance(getattr(event, "source", None), dict) else None),
        )
        self.pending_verifications[event.sender] = pending
        sent = await self.bot.command_manager.send_dm(
            public_key,
            f"MATRIX LINK {challenge}\nReply: VERIFY {challenge}",
            skip_user_rate_limit=True,
        )
        if not sent:
            self.pending_verifications.pop(event.sender, None)
            await self._send_matrix_status(
                room.room_id,
                "I could not reach that MeshCore public key. Send a MeshCore message to the bot first, then retry.",
            )
            return True
        await self._send_matrix_status(
            room.room_id,
            "A one-time challenge was sent over MeshCore. Reply to it with the requested text; "
            f"this request expires in {self.verification_timeout_seconds // 60} minutes.",
        )
        expiry_task = asyncio.create_task(self._expire_pending_verification(event.sender, pending.expires_at))
        self._verification_tasks[event.sender] = expiry_task
        return True

    async def _expire_pending_verification(self, matrix_user_id: str, expires_at: float) -> None:
        await asyncio.sleep(max(0, expires_at - time.time()))
        pending = self.pending_verifications.get(matrix_user_id)
        if pending and pending.expires_at <= time.time():
            if pending.transaction_id:
                with contextlib.suppress(Exception):
                    await self.client.cancel_key_verification(pending.transaction_id)
            self._clear_pending_verification(matrix_user_id)
            await self._send_matrix_status(pending.matrix_room_id, "Verification timed out.")

    async def _start_matrix_verification(self, pending: PendingVerification) -> None:
        try:
            response = await self.client.keys_query()
        except Exception as exc:
            self.logger.warning("Matrix device lookup failed for %s: %s", pending.matrix_user_id, exc)
            await self._send_matrix_status(pending.matrix_room_id, "I could not look up Matrix devices right now.")
            return
        devices = getattr(response, "device_keys", {}).get(pending.matrix_user_id, {})
        if not devices:
            await self._send_matrix_status(pending.matrix_room_id, "No Matrix devices were found for that account.")
            return
        if pending.matrix_device_id and pending.matrix_device_id in devices:
            device = devices[pending.matrix_device_id]
        else:
            pending.matrix_device_id = sorted(devices)[0]
            device = devices[pending.matrix_device_id]
        transaction_id = secrets.token_hex(16)
        try:
            response = await self.client.start_key_verification(device, transaction_id)
        except Exception as exc:
            self.logger.warning("Matrix verification start failed for %s: %s", pending.matrix_user_id, exc)
            await self._send_matrix_status(pending.matrix_room_id, "Matrix verification could not be started.")
            return
        if isinstance(response, ToDeviceError):
            await self._send_matrix_status(pending.matrix_room_id, "Matrix verification could not be started.")
            return
        pending.transaction_id = transaction_id
        pending.stage = "matrix_sas"
        device_note = (
            f" I selected Matrix device {pending.matrix_device_id}."
            if len(devices) > 1 else ""
        )
        await self._send_matrix_status(
            pending.matrix_room_id,
            "Matrix emoji verification has started." + device_note +
            " I will send my emoji sequence over MeshCore.",
        )

    async def _send_sas_over_meshcore(self, pending: PendingVerification) -> None:
        sas = self.client.key_verifications.get(pending.transaction_id or "")
        if not sas:
            return
        emoji_text = " ".join(emoji for emoji, _description in sas.get_emoji())
        sent = await self.bot.command_manager.send_dm(
            pending.meshcore_public_key,
            f"MATRIX EMOJIS {emoji_text}\nReply: YES {pending.challenge} or NO {pending.challenge}",
            skip_user_rate_limit=True,
        )
        if sent:
            pending.stage = "mesh_sas_confirmation"
            await self._send_matrix_status(
                pending.matrix_room_id,
                "Compare the emojis sent over MeshCore with the emojis shown by your Matrix client, "
                "then reply over MeshCore with YES or NO.",
            )
        else:
            await self._send_matrix_status(pending.matrix_room_id, "I could not send the emoji sequence over MeshCore.")

    async def _on_mesh_verification_message(self, event: Any, metadata: Any = None) -> None:
        payload = copy.deepcopy(getattr(event, "payload", None))
        if not payload:
            return
        public_key = self._resolve_meshcore_public_key(str(payload.get("pubkey_prefix", "")))
        if not public_key:
            return
        pending = self._find_pending_by_mesh_key(public_key)
        if not pending or pending.expires_at <= time.time():
            return
        text = str(payload.get("text", "")).strip()
        if pending.stage == "mesh_challenge":
            if text.upper() != f"VERIFY {pending.challenge}":
                return
            await self._start_matrix_verification(pending)
            return
        if pending.stage != "mesh_sas_confirmation":
            return
        if text.upper() == f"NO {pending.challenge}":
            with contextlib.suppress(Exception):
                await self.client.cancel_key_verification(pending.transaction_id or "", reject=True)
            await self._send_matrix_status(pending.matrix_room_id, "Verification was rejected over MeshCore.")
            self._clear_pending_verification(pending.matrix_user_id)
            return
        if text.upper() != f"YES {pending.challenge}":
            return
        try:
            response = await self.client.confirm_short_auth_string(pending.transaction_id or "")
        except Exception as exc:
            self.logger.warning("Matrix verification confirmation failed: %s", exc)
            await self._send_matrix_status(pending.matrix_room_id, "Matrix could not confirm the emoji verification.")
            return
        if isinstance(response, ToDeviceError):
            await self._send_matrix_status(pending.matrix_room_id, "Matrix could not confirm the emoji verification.")
            return
        pending.stage = "matrix_mac"

    def _channel_for_room(self, room_id: str) -> Optional[str]:
        for channel, mapped_room in self.channel_rooms.items():
            if mapped_room == room_id:
                return channel
        return None

    def _truncate(self, text: str) -> str:
        if len(text) <= self.max_message_length:
            return text
        return text[: self.max_message_length - 3].rstrip() + "..."

    async def _queue_matrix_notice(self, room_id: str, notice: str, channel: str) -> None:
        """Tell the Matrix room how an inbound message was handled."""
        await self._queue_message(room_id, f"[Matrix bridge] {notice}", channel)

    def _format_outbound(self, sender: str, text: str, channel: str) -> str:
        text = text.replace("@[", "@").replace("]", "")
        return self._truncate(f"[{channel}] {sender}: {text}")

    def _passes_filter(self, sender: str, text: str) -> tuple[str, str] | None:
        if self.filter_profanity == "drop" and (
            contains_profanity(sender, self.logger) or contains_profanity(text, self.logger)
        ):
            return None
        if self.filter_profanity == "censor":
            sender = censor(sender, self.logger)
            text = censor(text, self.logger)
        return sender, text

    async def start(self) -> None:
        if not self.enabled or (not self.channel_rooms and not self.verification_enabled):
            return
        client_config = AsyncClientConfig(
            encryption_enabled=self.encryption_enabled,
            store_sync_tokens=True,
        )
        self.client = AsyncClient(
            self.homeserver, self.user_id, device_id=self.device_id,
            config=client_config, store_path=self.store_path if self.encryption_enabled else None,
        )
        self.client.access_token = self.access_token
        self.client.add_event_callback(self._on_matrix_message, RoomMessageText)
        if self.verification_enabled and self.encryption_enabled:
            self.client.add_to_device_callback(self._on_matrix_verification_event, KeyVerificationEvent)
        self._running = True
        self._sync_task = asyncio.create_task(self._sync_matrix())
        if self.bot.meshcore:
            self.bot.meshcore.subscribe(EventType.CHANNEL_MSG_RECV, self._on_mesh_channel_message)
            if self.verification_enabled:
                self.bot.meshcore.subscribe(EventType.CONTACT_MSG_RECV, self._on_mesh_verification_message)
        if self.bridge_bot_responses and getattr(self.bot, "channel_sent_listeners", None) is not None:
            self.bot.channel_sent_listeners.append(self._on_mesh_channel_message)
        self._queue_processor_task = asyncio.create_task(self._process_message_queues())

    async def _sync_matrix(self) -> None:
        try:
            await self.client.sync_forever(timeout=30000, full_state=True)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.logger.error("Matrix sync stopped: %s", exc, exc_info=True)

    async def stop(self) -> None:
        self._running = False
        if getattr(self.bot, "channel_sent_listeners", None) is not None:
            with contextlib.suppress(ValueError):
                self.bot.channel_sent_listeners.remove(self._on_mesh_channel_message)
        for task_name in ("_sync_task", "_queue_processor_task"):
            task = getattr(self, task_name, None)
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        tasks = list(self._verification_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._verification_tasks.clear()
        if self.client:
            await self.client.close()
            self.client = None

    async def on_transport_reconnected(self) -> None:
        if not self._running or not getattr(self.bot, "meshcore", None):
            return
        self.bot.meshcore.subscribe(EventType.CHANNEL_MSG_RECV, self._on_mesh_channel_message)
        if self.verification_enabled:
            self.bot.meshcore.subscribe(EventType.CONTACT_MSG_RECV, self._on_mesh_verification_message)

    async def _on_mesh_channel_message(self, event: Any, metadata: Any = None) -> None:
        payload = copy.deepcopy(getattr(event, "payload", None))
        if not payload:
            return
        channel = self.bot.channel_manager.get_channel_name(payload.get("channel_idx", 0))
        if not channel or channel.lstrip("#").lower() in ("dm", "direct", "private"):
            return
        room_id = next(
            (room for name, room in self.channel_rooms.items()
             if name.lstrip("#").lower() == channel.lstrip("#").lower()),
            None,
        )
        if not room_id:
            return
        sender, separator, body = payload.get("text", "").partition(":")
        if not separator:
            sender, body = "Unknown", payload.get("text", "")
        filtered = self._passes_filter(sender.strip(), body.strip())
        if filtered is None:
            return
        await self._queue_message(room_id, self._format_outbound(*filtered, channel), channel)

    async def _queue_message(self, room_id: str, body: str, channel: str) -> None:
        self.message_queues.setdefault(room_id, []).append(QueuedMessage(room_id, body, channel))
        self.send_times.setdefault(room_id, deque())

    async def _process_message_queues(self) -> None:
        while self._running:
            now = time.time()
            for room_id, queue in list(self.message_queues.items()):
                if not queue or (self.send_times[room_id] and now - self.send_times[room_id][-1] < 1.0):
                    continue
                message = next((item for item in queue if now >= item.next_retry_at), None)
                if message is None:
                    continue
                success = now - message.first_queued > self.max_queue_age or await self._send_room_message(message)
                if success:
                    queue.remove(message)
                    self.send_times[room_id].append(now)
                else:
                    message.retry_count += 1
                    if message.retry_count > self.max_retries:
                        queue.remove(message)
                    else:
                        message.next_retry_at = now + self.retry_delay_base * (2 ** (message.retry_count - 1))
            await asyncio.sleep(0.1)

    async def _send_room_message(self, message: QueuedMessage) -> bool:
        try:
            response = await self.client.room_send(
                room_id=message.room_id,
                message_type="m.room.message",
                content={"msgtype": "m.text", "body": message.body},
            )
            return hasattr(response, "event_id")
        except Exception as exc:
            self.logger.warning("Matrix send failed for %s: %s", message.channel_name, exc)
            return False

    async def _on_matrix_message(self, room: Any, event: Any) -> None:
        if await self._handle_matrix_control_message(room, event):
            return
        channel = self._channel_for_room(room.room_id)
        if (
            not channel
            or channel.lstrip("#").lower() not in self.inbound_channels
            or event.sender == self.user_id
        ):
            return
        filtered = self._passes_filter(event.sender, event.body)
        if filtered is None:
            return
        sender, body = filtered
        original_message = f"{sender}: {body}"
        message = self._truncate(original_message)
        was_truncated = message != original_message
        chunks = self.bot.command_manager.split_text_into_utf8_chunks(
            message, MESHCORE_CHANNEL_MESSAGE_MAX_BYTES
        )
        sent = await self.bot.command_manager.send_channel_messages_chunked(
            channel, chunks, skip_user_rate_limit=True,
        )
        if was_truncated:
            await self._queue_matrix_notice(
                room.room_id,
                f"Message from {sender} was truncated to {self.max_message_length} characters "
                "before sending to MeshCore.",
                channel,
            )
        elif len(chunks) > 1:
            await self._queue_matrix_notice(
                room.room_id,
                f"Message from {sender} was split into {len(chunks)} MeshCore messages.",
                channel,
            )
        if not sent:
            await self._queue_matrix_notice(
                room.room_id,
                "Message could not be sent to MeshCore, possibly because of radio or rate limits.",
                channel,
            )

    async def _on_matrix_verification_event(self, event: Any) -> None:
        transaction_id = getattr(event, "transaction_id", None)
        if not transaction_id:
            return
        pending = next(
            (candidate for candidate in self.pending_verifications.values()
             if candidate.transaction_id == transaction_id),
            None,
        )
        if not pending or event.sender != pending.matrix_user_id:
            return
        if isinstance(event, KeyVerificationAccept):
            sas = self.client.key_verifications.get(transaction_id)
            if not sas:
                return
            response = await self.client.to_device(sas.share_key())
            if isinstance(response, ToDeviceError):
                await self._send_matrix_status(pending.matrix_room_id, "Matrix verification could not exchange keys.")
            return
        if isinstance(event, KeyVerificationKey):
            await self._send_sas_over_meshcore(pending)
            return
        if isinstance(event, KeyVerificationMac):
            sas = self.client.key_verifications.get(transaction_id)
            if not sas:
                return
            if not sas.verified:
                await self._send_matrix_status(
                    pending.matrix_room_id,
                    "Matrix verification failed because the received key confirmation was invalid.",
                )
                self._clear_pending_verification(pending.matrix_user_id)
                return
            try:
                response = await self.client.to_device(sas.get_mac())
            except Exception as exc:
                self.logger.warning("Matrix verification MAC exchange failed: %s", exc)
                await self._send_matrix_status(pending.matrix_room_id, "Matrix verification could not complete.")
                return
            if isinstance(response, ToDeviceError):
                await self._send_matrix_status(pending.matrix_room_id, "Matrix verification could not complete.")
                return
            self._linked_meshcore_keys[pending.matrix_user_id] = pending.meshcore_public_key
            self._save_linked_identities()
            await self._send_matrix_status(
                pending.matrix_room_id,
                f"Verification succeeded. This Matrix account is now linked to MeshCore {pending.meshcore_public_key[:12]}....",
            )
            await self.bot.command_manager.send_dm(
                pending.meshcore_public_key,
                "Matrix verification succeeded; your MeshCore identity is linked.",
                skip_user_rate_limit=True,
            )
            self._clear_pending_verification(pending.matrix_user_id)
            return
        if isinstance(event, KeyVerificationCancel):
            await self._send_matrix_status(
                pending.matrix_room_id,
                f"Verification failed or was cancelled: {getattr(event, 'reason', 'unknown reason')}.",
            )
            self._clear_pending_verification(pending.matrix_user_id)
