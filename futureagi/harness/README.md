# harness

Point it at an agent. It reads that agent's source, builds a real environment its tools act on,
writes test scenarios that are each proved before they are kept, and runs them.

Installs and runs on its own — six third-party packages, no dependency on the rest of this
repository. `src/harness/README.md` is the deeper reference; this file is about running it as a
service and about where its boundaries are.

## Why it exists

Mocked tool responses answer every call the same way. An agent that cancels an order which was
never placed is told it succeeded, and the test written to catch that passes.

Here the environment answers truthfully, including a truthful refusal, so the agent meets the
same world a user would put it in. The distinction the design rests on:

- **A refusal is the world working** — "that order is already delivered, it cannot be cancelled".
- **A crash is our bug** — a stack trace means the world was built wrong.

## The four stages

Each is a model session with a small tool surface and its method in a markdown skill under
`src/harness/skills/`. The model decides what to try; the tools decide what is true. Nothing
reaches disk except through a tool that validated it first.

| stage | produces |
|---|---|
| understand | `contract.json` — the tools with exact argument names and permitted values, the hard rules, the real data |
| build | the world its tools act on, one handler per tool, the simulator prompt, the sub-goal catalogue with each check written as code |
| scenarios | one folder per scenario, each proved by three gates before it is kept |
| run | grades from world state plus every tool call |

A scenario owns a folder, and the code in it is code rather than strings inside JSON:

```
scenarios/<name>/
    scenario.json     the instruction, the reference solution, which sub-goals it names
    setup.py          def setup(world)
    ready.py          def ready(world)
    checks/<goal>.py  def check(world, calls), runnable on its own against a finished run
```

**Three gates, all pure code, no model involved.** A scenario is kept only if it passes all
three: **ready** (the world holds what the scenario presumes), **solvable** (the reference
solution passes the scenario's own checks), and **not vacuous** (running *nothing* must fail
those checks). The third is the one that earns its keep — a check that passes while the agent
did nothing grades nothing while reporting a result.

## Two servers, two ports

This trips people up, so it is stated before anything else about running it. The harness listens
on **two** ports, from **two** different HTTP servers, for two unrelated audiences.

| | what | who talks to it | port |
|---|---|---|---|
| `ui/server.py` | **FastAPI**, served by uvicorn. The chat, the event stream, and the endpoints that read sessions, contracts, worlds, scenarios and runs | a person, or a front end | `HARNESS_PORT`, default **8777** |
| `WorldWebhook` in `run/voice.py` | **stdlib `http.server.HTTPServer`** in a daemon thread. Not FastAPI, not uvicorn, not part of the app above | **the agent under test**, delivering its tool calls | chosen at construction, **ephemeral by default** |

They are deliberately separate. The UI server is long-lived for a session; the webhook comes up
around a voice run and swaps which world is bound between scenarios.

**Two things follow that matter when running this in a container:**

- **Expose both ports.** Publishing 8777 alone gives you a working UI and a voice run whose tool
  calls never arrive.
- **Pin the webhook's port, and bind it on `0.0.0.0`.** It defaults to
  `WorldWebhook(host="127.0.0.1", port=0)` — the OS picks the port, which was fine when a tunnel
  discovered the public URL after binding. It is not fine when a sibling container needs a stable
  address to call: loopback is that container's own, and an ephemeral port cannot be published or
  configured ahead of time. Both are constructor arguments, so this is a call-site change.

**It is a standalone FastAPI app, not a Django app.** It does not mount into the backend's ASGI
stack and does not share its middleware, authentication or workspace context. Anything needing
those should sit in front of it rather than inside it.

## How the world is wired, and who owns it

**The harness owns the world. Nothing else does.** This is the most common misunderstanding, so
it is spelled out here.

```
   simulated caller  ──────►  agent under test
   (a model behind                  │
    STT and TTS)                    │  its tools are pointed here
                                    ▼
                            world webhook  (this service)
                                    │
                                    ▼
                            world.handle_tool_call()
                                    │
                                    ▼
                            handlers/<tool>.py   ── real state, real refusals
```

A voice run wires it like this:

1. Restore the world for the scenario and apply its `setup`.
2. Start a `WorldWebhook` and `bind` the world to it.
3. Point the agent's tools at that webhook.
4. Place the call. Every tool the agent calls is answered by the world.
5. Grade from what the world holds afterwards, plus every call that was made.

**The platform's hosted simulation runner does not handle the world, and cannot.** Its job spec
has no field for one — a voice job carries a target, a simulator and a persona dataset, and
nothing else. The world is not passed to it; it is an HTTP service the *agent* calls. Sending a
run through the runner without this service running does not fail loudly — the agent's tools
simply hit whatever they normally hit, and the run grades a conversation instead of an outcome.

So the division is:

| | owns |
|---|---|
| this service | the contract, the world, the scenarios, the gates, grading on state |
| hosted runner | driving a simulated caller, personas, transport, reporting |

## What it grades on

Three kinds of verdict: **code** (a `checks/<goal>.py` function returned true or false), **eval**
(a platform eval scored the run), and **judged** (a model judged it, given the whole run record).

Most sub-goals are settled by code. That is deliberate — a check you can read and re-run beats a
score you have to trust. It is also what lets a run assert "the order is actually cancelled"
rather than "the agent said it cancelled the order".

## Running it

A session is a folder. Nothing is held in memory that is not also on disk, so refreshing the
page, restarting the process or coming back tomorrow all resume by reading it.

```
artifacts/sessions/<session-id>/
    session.json  chat.jsonl  contract.json  world.py  handlers/
    state.json    sub_goals.json  simulator_prompt.md
    scenarios/<name>/   runs/<run-id>/
```

Locally:

```bash
python -m venv .venv && .venv/bin/pip install -e ".[ui]"
npm install -g @anthropic-ai/claude-code     # the SDK drives this as a subprocess
.venv/bin/python ui/server.py                # http://localhost:8777
```

Then say what to test, and one message is enough to start.

### Configuration

| variable | what it does |
|---|---|
| `HARNESS_HOST` / `HARNESS_PORT` | where the server binds. Defaults to loopback on 8777; a container must set the host to `0.0.0.0` |
| `ALK_HARNESS_MODEL` | which model the stages run on. Use Sonnet — smaller models misread `modality`, and modality decides the entire run path |
| `GOOGLE_APPLICATION_CREDENTIALS`, `CLOUD_ML_REGION` | provider credentials, read from the environment and never from source |
| `ALK_DOCKER_NETWORK` | when set, containers this service starts join that network and are addressed by container name instead of loopback |

## Running it in a container

Everything a container author needs, in one place. Each item here is something that fails in a
way that does not obviously point at its cause.

**1. Three runtimes have to be in the image.**

| runtime | why |
|---|---|
| Python 3.11+ | the harness itself |
| **Node** | the Agent SDK drives the Claude Code CLI (`@anthropic-ai/claude-code`) as a **subprocess**. Without it **every stage fails at its first model call**, and nothing in the Python dependencies reveals the requirement |
| Docker **client** | only for worlds whose store is a real database engine. The client alone — it talks to a daemon, it must never run one |

**2. Expose both ports** — see "Two servers, two ports" above. The UI port alone gives you a
working chat and a voice run whose tool calls silently never arrive.

**3. Bind on `0.0.0.0`, both servers.** `HARNESS_HOST=0.0.0.0` for the UI. The webhook takes its
host and port as constructor arguments and defaults to loopback on an ephemeral port; give it a
fixed port too, or nothing outside can be configured to call it.

**4. Mount a volume for `artifacts/`, and mind the working directory.** Sessions are written to
`artifacts/sessions/`, a path **relative to the working directory** — so the mount point depends
on `WORKDIR`. If sessions vanish between restarts, this is why. It is files only; there is no
database.

**5. Credentials come from the environment, never from source** — and a host path inside an env
file means nothing in the container. If a variable names a credentials *file*, mount the file and
repoint the variable at where it now lives.

**6. To start containers of its own**, set `DOCKER_HOST` at a socket proxy rather than mounting
the daemon socket, and set `ALK_DOCKER_NETWORK` to the shared network name. With that set, the
harness attaches the containers it starts to that network and addresses them **by container
name**; without it they are published on loopback, which from inside a container is its own.

**7. Use Sonnet.** Smaller models misread `modality`, and modality decides the entire run path.

**8. Do not restart it mid-suite.** A run in flight is lost. Long suites are the normal case — a
30-scenario voice suite took 69 minutes.

## Storage

Files, on a volume. No database, no tables, no migrations. Everything a session generates is
loaded as text and executed, so none of it needs to be importable from disk — but the folder is
the source of truth, and that is what makes a session something you can zip and hand to someone
else.

Recordings are the exception worth planning for: a measured session was 891 MB, of which 888 MB
was audio and under 700 KB was everything defining the environment and its scenarios.

## Known limits

- Ten to twenty scenarios in one pass works; a hundred does not.
- A stage cannot hand back to an earlier stage on its own.
- A file-backed agent gets a sampled world, not its full dataset.
- Browser-based worlds are registered but stubbed.
- The chat path rebuilds a replica of the agent from its contract rather than talking to the real
  thing. Do not use it to demonstrate what the harness does.
