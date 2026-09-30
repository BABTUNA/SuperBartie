"""Talk to the MCP server over stdio the way a client would: list tools, then
call the read-only ones against a running Bartie api. Exits non-zero if the
tool list drifts from openapi.yaml or a call fails.

    BARTIE_API_URL=http://localhost:8088 uv run python tests/smoke_client.py
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys

import yaml
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

ROOT = pathlib.Path(__file__).resolve().parents[1]


def expected_tools() -> set[str]:
    spec = yaml.safe_load((ROOT / "openapi.yaml").read_text())
    return {
        op["operationId"]
        for methods in spec["paths"].values()
        for m, op in methods.items()
        if m in {"get", "post", "put", "patch", "delete"}
    }


async def main() -> int:
    # Stdio subprocesses do not inherit the environment by default; pass the
    # api url through explicitly, exactly as the plugin's .mcp.json does.
    env = {k: v for k, v in os.environ.items() if k in {"PATH", "HOME", "BARTIE_API_URL", "BARTIE_API_TOKEN"}}
    transport = StdioTransport(command="uv", args=["run", "--directory", str(ROOT), "python", "server.py"], env=env)
    async with Client(transport) as client:
        tools = {t.name for t in await client.list_tools()}
        want = expected_tools()
        if tools != want:
            print(f"tool drift: server={sorted(tools)} spec={sorted(want)}")
            return 1
        print(f"tools ({len(tools)}): {', '.join(sorted(tools))}")

        for name, args in [
            ("pipeline_list", {}),
            ("pipeline_detail", {"uuid": "bartie"}),
            ("pipeline_usage", {"uuid": "bartie"}),
            ("pipeline_error_logs", {"uuid": "bartie"}),
            ("pipeline_verify", {"uuid": "bartie", "timeout": "10s"}),
            ("destination_ask", {"q": "which animals are lions?", "mode": "live"}),
        ]:
            res = await client.call_tool(name, args)
            body = res.structured_content if res.structured_content is not None else res.data
            text = json.dumps(body, default=str)
            print(f"{name}: {text[:220]}{'...' if len(text) > 220 else ''}")

        # The one mutation must exist but is not exercised here.
        assert "pipeline_update_status" in tools
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
