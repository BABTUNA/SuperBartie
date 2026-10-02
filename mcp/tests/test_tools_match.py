"""The skills and the agent prompt are instructions about tools. This test
keeps them honest in both directions: every tool a skill names must exist in
the OpenAPI contract, and every tool the contract exposes must be mentioned by
at least one skill or the agent, so a new tool does not ship undocumented and
a renamed tool does not leave stale advice behind.

This is the check artie-mcp lacks: its monitoring skill told agents no
error-log tool existed after pipeline_error_logs had shipped in the contract.
"""

import pathlib
import re
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
PLUGIN = ROOT

TOOL_RE = re.compile(r"`([a-z]+(?:_[a-z]+)+)`")


def contract_tools() -> set[str]:
    spec = yaml.safe_load((ROOT / "openapi.yaml").read_text())
    return {
        op["operationId"]
        for methods in spec["paths"].values()
        for method, op in methods.items()
        if method in {"get", "post", "put", "patch", "delete"}
    }


def prose_files() -> list[pathlib.Path]:
    return sorted(PLUGIN.glob("skills/*/SKILL.md")) + sorted(PLUGIN.glob("agents/*.md"))


def tools_in(path: pathlib.Path) -> set[str]:
    return set(TOOL_RE.findall(path.read_text()))


class ToolsMatchContract(unittest.TestCase):
    def test_every_named_tool_exists(self):
        tools = contract_tools()
        for path in prose_files():
            named = tools_in(path)
            # Field names in backticks look like tool names too; only flag
            # things shaped like <resource>_<verb> that the contract does not
            # know AND that are not obviously JSON fields.
            unknown = {
                t for t in named - tools
                if not t.startswith(("has_", "is_")) and t not in KNOWN_FIELDS
            }
            self.assertFalse(unknown, f"{path.relative_to(ROOT)} names tools not in openapi.yaml: {sorted(unknown)}")

    def test_every_tool_is_documented(self):
        mentioned = set().union(*(tools_in(p) for p in prose_files()))
        missing = contract_tools() - mentioned
        self.assertFalse(missing, f"tools in openapi.yaml no skill or agent mentions: {sorted(missing)}")

    def test_operation_ids_use_artie_names_where_they_exist(self):
        # These are Artie MCP's names; if any drift, an agent trained on
        # theirs stops recognizing ours.
        for name in ("pipeline_list", "pipeline_detail", "pipeline_usage", "pipeline_error_logs", "pipeline_update_status"):
            self.assertIn(name, contract_tools())


# Backticked snake_case identifiers in the prose that are JSON fields or
# metadata columns, not tools.
KNOWN_FIELDS = {
    "__bartie_commit_ts",
    "__bartie_updated_at",
}


if __name__ == "__main__":
    unittest.main()
