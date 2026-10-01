# Phase 8: MCP Server

Goal: operate Bartie from Claude Code with the same tool names Artie MCP uses, generated from an API contract the same way artie-mcp generates its tools, plus two tools their product cannot offer because it cannot read the destination.

This doc describes the code as it is. It is a thin layer over the [control API](phase-6-control-api.md); `destination_ask` comes from [phase 7](phase-7-vector-destination.md).

Done when (verified locally):

```bash
cd mcp && uv run python -m unittest discover -s tests            # skills and contract agree, both directions
BARTIE_API_URL=http://localhost:8088 uv run python tests/smoke_client.py   # 7 tools listed, 6 read-only ones called over stdio

claude plugin marketplace add BABTUNA/Bartie
claude plugin install bartie@bartie                              # then ask: "is my pipeline behind?"
```

## The idea

artie-mcp is not hand-written tools. Its `server.py` loads a policy contract and FastMCP generates one tool per allowed route of Artie's OpenAPI spec; `plugins/artie/` ships a Claude Code plugin whose skills tell the agent how to sequence them. Bartie does the same over its own API. Because the control API (phase 6) used Artie's paths, the operationIds are Artie's tool names.

## Layout

```text
.claude-plugin/marketplace.json     repo root: `claude plugin marketplace add BABTUNA/Bartie`
mcp/
├── openapi.yaml                    hand-written contract, 7 operations, operationId = tool name
├── server.py                       FastMCP.from_openapi(spec, httpx client -> $BARTIE_API_URL), stdio
├── pyproject.toml, uv.lock         fastmcp 4, httpx, pyyaml
├── tests/
│   ├── test_tools_match.py         skills <-> contract drift test (both directions)
│   └── smoke_client.py             stdio client: list tools, call the six read-only ones against a live api
└── plugins/bartie/
    ├── .claude-plugin/plugin.json
    ├── .mcp.json                   uv run --directory ${CLAUDE_PLUGIN_ROOT}/../.. python server.py
    ├── agents/bartie.md
    └── skills/
        ├── monitoring/SKILL.md     which call answers which question + the bottleneck triage table
        └── verify/SKILL.md         when to pass timeout, how to read a mismatch
```

## Tools

| Tool | Route | Artie has it |
| --- | --- | --- |
| `pipeline_list` | `GET /pipelines` | yes |
| `pipeline_detail` | `GET /pipelines/{uuid}` | yes |
| `pipeline_usage` | `GET /pipelines/{uuid}/usage` | yes, without the three lag fields |
| `pipeline_error_logs` | `GET /pipelines/{uuid}/error-logs` | yes |
| `pipeline_update_status` | `POST /pipelines/{uuid}/status` | yes; the only mutation |
| `pipeline_verify` | `POST /pipelines/{uuid}/verify` | no: their product cannot read the destination |
| `destination_ask` | `POST /ask` | no |

The `/demo/*` routes are deliberately not in the contract. An agent has no business writing to the source.

## The monitoring skill's triage table

```text
readerLagBytes growing, backlog flat, mergeMs flat   -> reader or source: slot blocked, reader down, write burst
readerLagBytes flat, backlog growing, mergeMs flat   -> writer paused or down; check processes.writer and error logs
backlog growing, mergeMs rising                      -> destination is the bottleneck
all flat, latency ~1-2s                              -> healthy; that is the flush-interval floor
mergeMs null, writerReachable false                  -> writer metrics endpoint down; status is unknown, not paused
```

Trends need two calls a minute apart. The skill says so, and stops at diagnosis: it never calls `pipeline_update_status`.

## The drift test

`test_tools_match.py` parses backticked snake_case names out of the skills and the agent prompt and checks them against the operationIds, in both directions: every tool a skill names must exist, and every tool in the contract must be mentioned somewhere. It exists because artie-mcp's monitoring skill kept telling agents there was no error-log tool after `pipeline_error_logs` had shipped in its contract.

## Gotchas, as found

- FastMCP names tools from `mcp_names`, keyed by operationId. Without it the names are derived from paths.
- Stdio subprocesses do not inherit the parent's environment. The smoke client passes `BARTIE_API_URL` explicitly, as the plugin's `.mcp.json` does.
- A response schema using `allOf` (`PipelineDetail`) comes back wrapped as `{"result": {...}}`. Harmless; agents read through it.
- The server is local and stateless. No hosted endpoint, no OAuth; that is Artie's product surface.

## Components

| Piece | State |
| --- | --- |
| `mcp/openapi.yaml` | new: the contract, 7 operations |
| `mcp/server.py` | new: FastMCP server generated from the contract, stdio |
| `mcp/plugins/bartie/` | new: plugin manifest, `.mcp.json`, agent prompt, `monitoring` and `verify` skills |
| `mcp/tests/` | new: drift test and stdio smoke client |
| `.claude-plugin/marketplace.json` | new, at the repo root |

## Explicitly out of scope

A hosted MCP endpoint, OAuth, exposing the `/demo/*` routes as tools, more than one pipeline.
