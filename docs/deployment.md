# Container deployment

Parley can run as a local server process or as an app container. The manager image is a single static `parley` binary with embedded web assets. At runtime it persists state under an explicit data root and reads the target repository from an explicit repository root.

## Build the manager image

From the repository root:

```sh
podman build \
  -f build/manager/Dockerfile \
  -t localhost/parley-manager:dev \
  .
```

## Run the UI against mounted roots

Create or choose two host paths:

- `/absolute/path/to/parley-data` — durable Parley state, SQLite, artifacts, workspaces, and logs.
- `/absolute/path/to/repository` — the repository Parley should inspect and run against.

Start the app container:

```sh
podman run --rm \
  --name parley-manager \
  --publish 127.0.0.1:8080:8080 \
  --userns keep-id \
  --user "$(id -u):$(id -g)" \
  --env PARLEY_ADDR=0.0.0.0:8080 \
  --env PARLEY_DATA_DIR=/data \
  --env PARLEY_SOURCE_REPO=/workspace/repo \
  --volume /absolute/path/to/parley-data:/data:rw \
  --volume /absolute/path/to/repository:/workspace/repo:rw \
  localhost/parley-manager:dev
```

Open `http://127.0.0.1:8080`.

The `--userns keep-id` and `--user` flags make files written under the mounted data root owned by the invoking host user in rootless Podman. If your runtime or host policy handles bind-mount ownership differently, use any UID/GID that can write the data root and read/write the repository root. On SELinux-enforcing hosts, append `,Z` to each writable bind mount option, for example `:/data:rw,Z`.

## Optional reference material mount

If the run should receive read-only reference material, mount it and keep the container path in `PARLEY_REFERENCE_ROOT`:

```sh
podman run --rm \
  --name parley-manager \
  --publish 127.0.0.1:8080:8080 \
  --userns keep-id \
  --user "$(id -u):$(id -g)" \
  --env PARLEY_REFERENCE_ROOT=/workspace/reference \
  --volume /absolute/path/to/parley-data:/data:rw \
  --volume /absolute/path/to/repository:/workspace/repo:rw \
  --volume /absolute/path/to/reference:/workspace/reference:ro \
  localhost/parley-manager:dev
```

## Shared-instance settings (multiple concurrent users)

A single Parley instance can serve several people at once — for example a private
testing group. Two facts shape that deployment:

- There is **one local runner slot**: one run executes at a time and additional runs
  queue FIFO. This is expected, not a fault.
- Conversation turns and runs otherwise compete for the same host. Three TOML settings
  keep chat responsive while runs execute.

Write the settings to a file on the mounted data volume and point `PARLEY_CONFIG` at it:

```sh
--env PARLEY_CONFIG=/data/config.toml
```

Recommended starting values for a small shared instance (a handful of concurrent users):

```toml
[conversation]
# Concurrent chat turns across all conversations (default 1 — raise it for a group).
budget = 2

[execution]
# Total in-flight work (runs + chat turns) across the whole host.
# The default is 0 = no umbrella; a shared instance should set it explicitly.
global_max_concurrent = 3

# Slots of the umbrella reserved for chat turns only, so chat never
# starves behind runs. Must be lower than global_max_concurrent.
interactive_reserve = 1
```

Notes:

- `global_max_concurrent = 0` (the default) disables the cross-pool ceiling entirely;
  startup rejects an enabled value that does not exceed `interactive_reserve`.
- Chat turns also have a per-turn safety deadline (`[conversation] turn_deadline`,
  default `"15m"`): a stuck turn is cancelled, the conversation stays usable, and the
  next message starts fresh. Leave the default unless testing shows otherwise.
- When the ceiling holds a queued run back, the run log records a
  `queue.held_global_cap` event and the queue view shows live occupancy — a held run
  under load is the umbrella working, not a stall.
- Settings can also live at `.parley/config.toml` relative to the manager working
  directory, or globally at the user config dir (`parley/config.toml`); the
  `PARLEY_CONFIG` / `PARLEY_GLOBAL_CONFIG` variables override those paths.

## Runtime notes

- The image defaults to `PARLEY_ADDR=0.0.0.0:8080`, `PARLEY_DATA_DIR=/data`, and `PARLEY_SOURCE_REPO=/workspace/repo`.
- The default `noop` adapter starts the UI and exercises the harness without needing agent credentials.
- Full Pi-backed implementation and validation still require the same local substrate as process mode: a working rootless Podman setup, worker image, and explicit credential paths or volumes. Do not mount container runtime sockets or credentials unless the host operator has deliberately chosen that deployment shape.
- The published port above binds to loopback only. Use a different `--publish` address only when you also provide the intended network access controls. Parley has no built-in authentication: anyone who can reach the port is a full operator. For a shared instance, provide access control at the network layer (VPN, reverse proxy) and treat the surface as shared — all projects, conversations, and runs are visible to every user.
