# Phase 8: MCP Server

Goal: operate the pipeline from Claude Code with the tool names Artie MCP uses, generated from an API contract the way artie-mcp generates its tools.

Done when:

```bash
cd mcp && uv run python -m unittest discover -s tests                      # skills and contract agree
BARTIE_API_URL=http://localhost:8088 uv run python tests/smoke_client.py   # 7 tools listed and called over stdio

claude plugin marketplace add BABTUNA/SuperBartie
claude plugin install super-bartie@super-bartie                            # then ask "is my pipeline behind?"
```

## The core idea

artie-mcp has no hand-written tool functions. It loads an OpenAPI spec and FastMCP makes one tool per allowed route, then a plugin ships skills that tell the agent how to sequence the calls. This does the same over the phase 6 api. Because that api used Artie's paths, the operationIds are Artie's tool names.

```text
mcp/openapi.yaml ──FastMCP.from_openapi──▶ 7 tools ──stdio──▶ Claude Code
                                              │
                                              └──http──▶ control api (phase 6)
```

## Tools

| Tool | Route | Artie has it |
| --- | --- | --- |
| `pipeline_list` | `GET /pipelines` | yes |
| `pipeline_detail` | `GET /pipelines/{uuid}` | yes |
| `pipeline_usage` | `GET /pipelines/{uuid}/usage` | yes, without the three lag fields |
| `pipeline_error_logs` | `GET /pipelines/{uuid}/error-logs` | yes |
| `pipeline_update_status` | `POST /pipelines/{uuid}/status` | yes; the only mutation |
| `pipeline_verify` | `POST /pipelines/{uuid}/verify` | no: needs destination access |
| `destination_ask` | `POST /ask` | no |

The `/demo/*` routes are left out of the contract on purpose. An agent has no business writing to the source.

## Layout

```text
.claude-plugin/marketplace.json           repo root
mcp/openapi.yaml                          the contract, operationId = tool name
mcp/server.py                             FastMCP.from_openapi(spec, httpx -> $BARTIE_API_URL)
mcp/plugins/super-bartie/
├── .mcp.json, .claude-plugin/plugin.json
├── agents/super-bartie.md
└── skills/monitoring/SKILL.md            which call answers which question + the triage table
    skills/verify/SKILL.md                when to pass timeout, how to read a mismatch
mcp/tests/test_tools_match.py             the drift test
mcp/tests/smoke_client.py                 lists and calls the tools over stdio
```

## Core data shapes

### 1. The triage table in the monitoring skill

```text
readerLagBytes growing, backlog flat, mergeMs flat   -> reader or source
readerLagBytes flat, backlog growing, mergeMs flat   -> writer paused or down
backlog growing, mergeMs rising                      -> destination is slow
all flat, latency ~1-2s                              -> healthy, that is the flush interval
```

This is the part Artie's skill cannot have, because their api returns only the latency. The skill stops at diagnosis and never calls `pipeline_update_status`.

### 2. The drift test

It reads the backticked tool names out of the skills and the agent prompt and checks them against the contract, both directions: every tool a skill names must exist, and every tool in the contract must be mentioned by a skill. A renamed or new tool fails the test instead of leaving stale advice.

## Gotchas found while building

- FastMCP names tools from `mcp_names`, keyed by operationId. Without it the names come from the paths.
- A stdio subprocess does not inherit the parent's environment. `BARTIE_API_URL` has to be passed explicitly, as `.mcp.json` does.
- A response schema that uses `allOf` comes back wrapped as `{"result": {...}}`. Harmless.

## Components

| Piece | What changes |
| --- | --- |
| `mcp/openapi.yaml`, `mcp/server.py` | new: contract and the generated server |
| `mcp/plugins/super-bartie/` | new: plugin, agent prompt, two skills |
| `mcp/tests/` | new: drift test and smoke client |
| `.claude-plugin/marketplace.json` | new, at the repo root |

## Explicitly out of scope

A hosted endpoint, OAuth, exposing the demo routes as tools, more than one pipeline.
