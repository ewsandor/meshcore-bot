# Matrix Bridge

The Matrix bridge is a built-in service plugin that uses `matrix-nio` to send
MeshCore channel messages to Matrix rooms. Matrix-to-MeshCore routing is
disabled by default and can be enabled for individual mapped channels.

The normal project installation procedures install the Matrix dependency. If
`matrix-nio` is not installed, the service logs a clear error and remains
disabled; other MeshCore services continue to start.

## Configuration

Create a dedicated Matrix account and obtain an access token. Set
`homeserver`, `user_id`, and `access_token` in `[MatrixBridge]`, or provide the
token through `MATRIX_ACCESS_TOKEN`. Never commit a real token.

Use a stable `device_id` and persistent `store_path` when E2E encryption is
enabled. Invite the bot account to each room and grant it permission to send
messages. Then add mappings such as:

```ini
[MatrixBridge]
enabled = true
homeserver = https://matrix.example.org
user_id = @meshcore-bot:example.org
access_token = local-secret
device_id = MESHCOREBOT
store_path = data/matrix-store
encryption_enabled = true
bridge.Public = !roomid:example.org
inbound.Public = false
```

Set `inbound.<channel> = true` only for channels that should accept Matrix
messages. The bridge ignores the bot's own Matrix messages to prevent loops,
never bridges MeshCore DMs, and applies the configured profanity filter in both
directions. Matrix messages routed to MeshCore are split into ordered UTF-8
chunks no larger than the MeshCore 133-byte channel-message limit and use the
bot's standard chunk pacing and transmission rate limiting.