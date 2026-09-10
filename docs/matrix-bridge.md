# Matrix Bridge

The Matrix bridge is a built-in service plugin that uses `matrix-nio` to send
MeshCore channel messages to Matrix rooms. Matrix-to-MeshCore routing is
disabled by default and can be enabled for individual mapped channels.

## Fedora installation

Use the bot's project virtual environment so the system Python remains managed
by Fedora:

```bash
sudo dnf install python3 python3-pip python3-virtualenv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install 'matrix-nio[e2e]>=0.25.2'
```

For a normal source checkout, installing the project also installs the Matrix
dependency:

```bash
python -m pip install -e .
```

If `matrix-nio` is not installed, the service logs a clear error and remains
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
directions.