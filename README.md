# runner-hive

**One small Rust daemon that runs GitHub Actions jobs for hundreds of repositories,
instead of one heavyweight self-hosted runner per repository.**

GitHub only lets a personal account attach self-hosted runners **per repository**
(organizations get org-level runners; users don't). With ~150 repos that means
~150 copies of the official runner: each one a .NET `Runner.Listener` process,
usually in its own container, sitting idle almost all of the time while holding
thousands of file handles and ~100 MB of memory.

runner-hive keeps the per-repo *registrations* GitHub requires, but serves all of
them from one process:

```
                     ┌──────────────────────── runner-hive (Rust) ──────────────────────┐
 GitHub              │                                                                  │
 broker  ◄─ long-poll ┤ repo-1 listener ─┐                                               │
 broker  ◄─ long-poll ┤ repo-2 listener ─┤      ┌──────────────┐    while a job runs:    │
   ...               │       ...        ├────► │ max_jobs slots├──► Runner.Worker (official)
 broker  ◄─ long-poll ┤ repo-N listener ─┘      └──────────────┘    spawned per job       │
                     └──────────────────────────────────────────────────────────────────┘
```

* **Idle cost per repo is one pending HTTP long-poll** (a few KB of memory), not a
  process. Requests share connections via HTTP/2 where the service offers it.
* **Jobs run in the official `Runner.Worker`**, the same binary GitHub's runner
  uses, so actions, `container:` jobs, service containers, caching, artifacts,
  OIDC, job summaries and logs behave exactly as on a normal self-hosted runner.
  It only exists while a job is running.
* **A global `max_jobs` limit** replaces "one runner per repo". When the hive is
  full, idle registrations report `Busy` so GitHub keeps new jobs queued.
* **Discovery**: point it at a user or org and it registers every repo you admin,
  picks up new repos, and prunes deleted ones.

### Measured

Idle, on one Windows 11 machine, official runner 2.338.0:

| | handles | working set | private memory | threads | TCP connections |
|---|---:|---:|---:|---:|---:|
| **one** official runner (`Runner.Listener`) | 542 | 73 MB | 22 MB | 20 | 1 |
| runner-hive serving **150** registrations | 300 | 25 MB | 12 MB | 8 | 3 |

150 official runners would be ~81,000 handles and ~11 GB of working set before
any container overhead. runner-hive used 2.3 s of CPU in its first 5 minutes,
mostly startup. The broker caps HTTP/2 streams at 100 per connection, so
listeners are spread at 50 per connection: 150 registrations, 3 sockets.

There is no Rust implementation of the runner protocol that I could find. The Go
project [github-act-runner](https://github.com/ChristopherHX/github-act-runner)
also multiplexes registrations, but it executes jobs with
[nektos/act](https://github.com/nektos/act), a reimplementation with known
compatibility gaps. runner-hive reimplements only the listener (the part whose
cost scales with repo count) and delegates execution to the real worker.

## Quick start

```bash
cargo install --git https://github.com/MattMills/runner-hive
runner-hive init                 # writes hive.toml
$EDITOR hive.toml                # set [[target]] owner = "you"
runner-hive sync --dry-run       # what would be registered
runner-hive sync                 # register runners
runner-hive run                  # serve jobs (Ctrl-C to stop cleanly)
```

The admin token comes from `$RUNNER_HIVE_TOKEN`, `$GITHUB_TOKEN`, `$GH_TOKEN`,
or `gh auth token`. It needs admin rights on the repos (classic PAT with `repo`,
or fine-grained with *Administration: read & write*). It is only used for
registration and discovery. Jobs never see it: the daemon scrubs it from the
worker's environment.

The first run downloads the official runner release for your platform into
`data_dir/packages` and verifies its SHA-256 against the release notes.

Then target the runners in workflows the usual way:

```yaml
jobs:
  build:
    runs-on: [self-hosted, hive]   # "hive" comes from labels = ["hive"]
```

## Configuration

`runner-hive init` writes a commented example. The important knobs:

| key | default | meaning |
|---|---|---|
| `max_jobs` | half the CPUs | jobs running at once across **all** repos |
| `runners_per_repo` | 1 | registrations per repo = max parallel jobs *per repo* (matrix builds) |
| `labels` | `[]` | extra labels; `self-hosted`, OS and arch are always added |
| `sync_interval_minutes` | 30 | re-discover repos and check for worker updates while running |
| `prune` | true | unregister repos that no longer match a target |
| `worker.version` | `latest` | official runner release used to run jobs |
| `worker.path` | — | use an existing extracted `actions-runner` directory instead |
| `worker.clean_workspace` | false | delete checkouts after each job (keeps tool/action caches) |
| `[env]` | — | extra environment for every job (the runner's `.env`) |

Targets:

```toml
[[target]]
owner = "MattMills"        # every repo of a user or org, one registration per repo
exclude = ["*-archive"]    # globs on the repo name; also `include`
include_public = false     # see Security below
include_forks = true
include_archived = false

[[target]]
repo = "MattMills/special" # a single repo
runners_per_repo = 4
labels = ["gpu"]

[[target]]
org = "my-org"             # ONE org-level registration serving every repo in the org
```

For organizations, prefer `org =`: a single registration covers every repo in
the org, which is what org-level runners are for. `owner =` exists for personal
accounts, where per-repo registration is the only option.

Commands: `init`, `sync [--dry-run]`, `run [--no-sync]`, `status [--remote]`,
`unregister <owner/repo>... | --all`, `install-worker`, `self-test`.

## How it works

The runner protocol isn't documented, so this was reverse engineered from the
[actions/runner](https://github.com/actions/runner) source
(`Runner.Listener`, `Runner.Common`, `Sdk`). The pieces:

1. **Registration** (`config.sh`): a registration token from the REST API is
   exchanged at `api.github.com/actions/runner-registration`. GitHub returns
   either a legacy pipelines tenant (agent is `POST`ed to
   `_apis/distributedtask/pools/{pool}/agents`) or the newer runner-admin flow
   (`api.github.com/actions/runners/register`). Both are implemented. Each
   registration gets its own 2048-bit RSA key; the public half is sent with it.
2. **Auth**: an RS256/PS256-signed JWT client assertion (`iss = sub = client id`,
   `aud` = token URL) is traded for a short-lived OAuth bearer token.
3. **Sessions & messages**: a session per registration, then a ~50 s long-poll for
   messages on the broker (`broker.actions.githubusercontent.com`), or on the
   legacy pipelines queue, which hands off to the broker with a `BrokerMigration`
   message. Legacy sessions encrypt bodies with an RSA-OAEP-wrapped AES key.
4. **Jobs**: a `RunnerJobRequest` message is acknowledged, then acquired from the
   run service (`acquirejob`). The broker keeps re-delivering a request until it is
   acquired, so acquisition happens before polling resumes.
5. **Execution**: the job message is handed, byte for byte, to `Runner.Worker
   spawnclient <in> <out>` over two inherited anonymous pipes. Framing is a
   little-endian i32 message type, an i32 length, then a UTF-16LE body. While it
   runs, the hive renews the job lock every 60 s (`renewjob`) and relays
   `JobCancellation` messages to the worker. The worker uploads logs and
   completes the job itself. If the worker crashes, the hive fails the job with
   the worker's output as an annotation.

`Runner.Worker` finds its root directory from its own path and reads `.runner`
there, so each concurrent job needs its own root. A **slot** is a hardlinked copy
of the runner package (near-zero disk) with that job's `.runner`. There are
`max_jobs` slots, not one per repo.

## Security

* **Self-hosted runners on public repositories can run code from fork pull
  requests.** Owner targets skip public repos unless `include_public = true`.
* All registrations share one machine and one OS user. A job from repo A can read
  files belonging to repo B's checkouts, and the hive's `data_dir` holds every
  registration's private key. That is the same trust model as running many
  official runners side by side under one user. Use it for repos you trust
  equally (e.g. all your own), or run separate hives per trust boundary.
* The admin token is stripped from job environments, and runner private keys are
  never written into job slots.

## Status

Early, but working end to end against github.com with the official runner
2.338.0 as the worker.

**Tested live** (Windows host, jobs from this repository's
[`hive-e2e`](.github/workflows/hive-e2e.yml) workflow):

* registration through the legacy tenant flow, PS256 OAuth, server-enforced
  move to broker sessions;
* a 3-job matrix on 2 registrations (2 in parallel, then the third), with
  `actions/checkout`, outputs, step summaries and log upload;
* `max_jobs = 1`: strict one-at-a-time execution with requests held until a
  slot frees, nothing lost or duplicated;
* cancelling a running job: the step is cancelled, `if: cancelled()` steps run,
  and the job ends `cancelled`;
* restart after a hard kill: stale sessions are cleaned up;
* 150 registrations in one process (numbers below);
* discovery against a real 150-repo account (`sync --dry-run`).

**Tested in CI**: unit tests, plus a self-test that starts the real
`Runner.Worker` over inherited pipes on Linux x64/arm64 (including a static musl
build), macOS and Windows.

**Implemented but not yet exercised live**: the runner-admin registration flow
(GitHub gave this account the legacy flow), the pipelines message queue with
encrypted messages and `BrokerMigration` hand-off (GitHub put new registrations
straight on the broker), worker-crash reporting, worker auto-update, pruning,
and running jobs on a Linux host (Linux is covered only by the IPC self-test).

**Not implemented**: the server-driven `RunnerRefreshConfig` migration (logged
and ignored; the existing registration keeps working), legacy
`PipelineAgentJobRequest` messages (old GHES), and running jobs as a different
OS user.

## License

MIT. Not affiliated with GitHub. Protocol knowledge comes from the MIT-licensed
[actions/runner](https://github.com/actions/runner) source.
