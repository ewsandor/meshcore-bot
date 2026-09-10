"""Tests for Matrix-to-MeshCore identity verification flow."""

from configparser import ConfigParser
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from modules.service_plugins.matrix_bridge_service import MatrixBridgeService, PendingVerification


def make_service(tmp_path):
    bot = MagicMock()
    bot.logger = MagicMock()
    bot.config = ConfigParser()
    bot.config.add_section("MatrixBridge")
    bot.config.set("MatrixBridge", "enabled", "true")
    bot.config.set("MatrixBridge", "homeserver", "https://matrix.example.org")
    bot.config.set("MatrixBridge", "user_id", "@bot:example.org")
    bot.config.set("MatrixBridge", "access_token", "test-token")
    bot.config.set("MatrixBridge", "store_path", str(tmp_path / "matrix-store"))
    bot.meshcore = MagicMock()
    bot.command_manager = MagicMock()
    bot.command_manager.send_dm = AsyncMock(return_value=True)
    service = MatrixBridgeService(bot)
    service._verification_state_path = tmp_path / "links.json"
    return service


@pytest.mark.asyncio
async def test_matrix_help_is_available(tmp_path):
    service = make_service(tmp_path)
    service._send_matrix_status = AsyncMock()
    handled = await service._handle_matrix_control_message(
        SimpleNamespace(room_id="!dm:example.org"),
        SimpleNamespace(sender="@alice:example.org", body="help"),
    )

    assert handled is True
    service._send_matrix_status.assert_awaited_once()
    assert "link meshcore" in service._send_matrix_status.await_args.args[1]


@pytest.mark.asyncio
async def test_matrix_link_sends_meshcore_challenge(tmp_path):
    service = make_service(tmp_path)
    room = SimpleNamespace(room_id="!dm:example.org")
    event = SimpleNamespace(
        sender="@alice:example.org",
        body="link meshcore " + "ab" * 32,
        source={"unsigned": {"device_id": "ALICEDEVICE"}},
    )

    assert await service._handle_matrix_control_message(room, event) is True
    service.bot.command_manager.send_dm.assert_awaited_once()
    recipient, challenge_message = service.bot.command_manager.send_dm.await_args.args[:2]
    assert recipient == "ab" * 32
    assert challenge_message.startswith("MATRIX LINK ")


@pytest.mark.asyncio
async def test_matching_meshcore_challenge_starts_matrix_verification(tmp_path):
    service = make_service(tmp_path)
    public_key = "cd" * 32
    pending = PendingVerification(
        matrix_user_id="@alice:example.org",
        matrix_room_id="!dm:example.org",
        meshcore_public_key=public_key,
        challenge="A1B2C3D4",
        created_at=1.0,
        expires_at=9999999999.0,
        matrix_device_id="ALICEDEVICE",
    )
    service.pending_verifications[pending.matrix_user_id] = pending
    service.bot.meshcore.contacts = {"contact": {"public_key": public_key}}
    device = SimpleNamespace(device_id="ALICEDEVICE")
    service.client = MagicMock()
    service.client.keys_query = AsyncMock(
        return_value=SimpleNamespace(device_keys={"@alice:example.org": {"ALICEDEVICE": device}})
    )
    service.client.start_key_verification = AsyncMock(return_value=SimpleNamespace())

    event = SimpleNamespace(payload={"pubkey_prefix": public_key[:8], "text": "VERIFY A1B2C3D4"})
    await service._on_mesh_verification_message(event)

    service.client.start_key_verification.assert_awaited_once_with(device, pending.transaction_id)
    assert pending.stage == "matrix_sas"


@pytest.mark.asyncio
async def test_wrong_meshcore_challenge_is_ignored(tmp_path):
    service = make_service(tmp_path)
    public_key = "ef" * 32
    pending = PendingVerification(
        matrix_user_id="@alice:example.org",
        matrix_room_id="!dm:example.org",
        meshcore_public_key=public_key,
        challenge="A1B2C3D4",
        created_at=1.0,
        expires_at=9999999999.0,
    )
    service.pending_verifications[pending.matrix_user_id] = pending
    service.bot.meshcore.contacts = {"contact": {"public_key": public_key}}
    service.client = MagicMock()
    service.client.keys_query = AsyncMock()

    await service._on_mesh_verification_message(
        SimpleNamespace(payload={"pubkey_prefix": public_key[:8], "text": "VERIFY WRONG"})
    )

    service.client.keys_query.assert_not_awaited()
    assert pending.stage == "mesh_challenge"
