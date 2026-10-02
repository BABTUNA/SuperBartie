"""Super Bartie MCP: tools generated from openapi.yaml, the way artie-mcp generates
its tools from Artie's OpenAPI spec plus a policy pin. No hand-written tool
functions; the contract is the spec. Tool names are the operationIds, which
are Artie MCP's names wherever an equivalent exists.

Run over stdio (what the Claude Code plugin does):

    BARTIE_API_URL=http://localhost:8088 uv run python server.py

Environment:
    BARTIE_API_URL    control api base URL (default http://localhost:8088, the compose live profile)
    BARTIE_API_TOKEN  bearer token, only needed if the api was started with one
"""

from __future__ import annotations

import os
import pathlib

import httpx
import yaml
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType

HERE = pathlib.Path(__file__).resolve().parent
API_URL = os.environ.get("BARTIE_API_URL", "http://localhost:8088").rstrip("/")
API_TOKEN = os.environ.get("BARTIE_API_TOKEN", "")

spec = yaml.safe_load((HERE / "openapi.yaml").read_text())

# Every operation in the spec is a tool, named by its operationId. Anything
# without one (there is nothing today) is excluded rather than auto-named.
_tools = {
    (method, path): op["operationId"]
    for path, methods in spec["paths"].items()
    for method, op in methods.items()
    if method in {"get", "post", "put", "patch", "delete"} and "operationId" in op
}


def _route_map(route, _default_type):
    if (route.method.lower(), route.path) in _tools:
        return MCPType.TOOL
    return MCPType.EXCLUDE


headers = {"Authorization": f"Bearer {API_TOKEN}"} if API_TOKEN else {}
client = httpx.AsyncClient(base_url=API_URL, headers=headers, timeout=60.0)

mcp = FastMCP.from_openapi(
    openapi_spec=spec,
    client=client,
    name="Super Bartie",
    mcp_names={name: name for name in _tools.values()},
    route_map_fn=_route_map,
)

if __name__ == "__main__":
    mcp.run()
