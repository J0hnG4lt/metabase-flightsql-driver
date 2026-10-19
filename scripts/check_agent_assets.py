"""Generate/check Claude and Codex adapters for shared workflows (Python 3.11+)."""
import argparse
import json
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = {
    "driver-dev": "Develop or debug Flight SQL driver code, authentication, metadata, SQL compilation, or the plugin manifest.",
    "metabase-api": "Use Metabase's CLI, official MCP, or REST API to inspect data, validate queries, or manage requested demo content.",
    "tutorial-generator": "Create a Flight SQL use-case tutorial with a running backend and verified screenshots for docs/tutorials.",
    "rebuild-driver": "Rebuild the local Flight SQL driver jar, restart Metabase, and verify plugin loading after code changes.",
    "e2e-test": "Run Flight SQL end-to-end tests against a local demo without deleting its existing application database.",
    "upgrade-metabase": "Upgrade the pinned Metabase version after reviewing its driver changelog and validating the built jar.",
}
COMMANDS = {"rebuild-driver", "e2e-test", "upgrade-metabase"}


def adapters():
    files = {"CLAUDE.md": "<!-- Shared project guidance lives in AGENTS.md. -->\n@./AGENTS.md\n"}
    for name, description in WORKFLOWS.items():
        body = (f"# {name}\n\nRead and follow [the shared workflow](../../../docs/agents/{name}.md).\n"
                "Resolve repository-relative commands from the Git root.\n")
        header = f"---\nname: {name}\ndescription: {description}\n---\n\n"
        files[f".agents/skills/{name}/SKILL.md"] = header + body
        if name not in COMMANDS:
            files[f".claude/skills/{name}/SKILL.md"] = header + body
        else:
            files[f".claude/commands/{name}.md"] = (
                f"---\ndescription: {description}\n---\n\n# {name}\n\n"
                f"Read and follow [the shared workflow](../../docs/agents/{name}.md).\n"
                "Use the target/version stated by the user when the workflow needs one.\n")
    instructions = ("Read docs/agents/driver-expert.md and the relevant shared workflow before driver work. "
                    "Resolve paths from the repository root. Do not assume the full demo stack is running.")
    description = "Flight SQL driver specialist for Metabase compatibility, auth, pooling, metadata and backend SQL behavior."
    files[".codex/agents/flightsql-driver-expert.toml"] = (
        'name = "flightsql-driver-expert"\n'
        f'description = "{description}"\n'
        f'developer_instructions = "{instructions}"\n')
    files[".claude/agents/flightsql-driver-expert.md"] = (
        f"---\nname: flightsql-driver-expert\ndescription: {description}\n---\n\n{instructions}\n")
    return files


def check(root=ROOT, write=False):
    errors = []
    expected = adapters()
    for relative, content in expected.items():
        path = root / relative
        if write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="\n")
        if not path.exists() or path.read_text(encoding="utf-8") != content:
            errors.append(f"stale/missing adapter: {relative} (run with --write)")
    for directory, pattern in [(".agents/skills", "*/SKILL.md"), (".claude/skills", "*/SKILL.md"),
                               (".claude/commands", "*.md"), (".codex/agents", "*.toml"),
                               (".claude/agents", "*.md")]:
        for path in (root / directory).glob(pattern):
            if path.relative_to(root).as_posix() not in expected:
                errors.append(f"unmapped agent asset: {path.relative_to(root)}")
    documents = [root / "AGENTS.md", *[root / f"docs/agents/{name}.md" for name in WORKFLOWS],
                 root / "docs/agents/driver-expert.md", root / "docs/agents/setup.md",
                 root / "docs/agents/compatibility.md"]
    documents += [root / p for p in expected if p.endswith(".md") and p != "CLAUDE.md"]
    for path in documents:
        if not path.is_file():
            errors.append(f"missing shared reference: {path.relative_to(root)}")
            continue
        text = path.read_text(encoding="utf-8")
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
            if "://" in target or target.startswith("#"):
                continue
            if not (path.parent / target.split("#")[0]).exists():
                errors.append(f"broken link in {path.relative_to(root)}: {target}")
    try:
        codex = tomllib.loads((root / ".codex/config.toml").read_text(encoding="utf-8"))
        claude = json.loads((root / ".mcp.json").read_text(encoding="utf-8"))
        if codex["mcp_servers"]["metabase"]["url"] != claude["mcpServers"]["metabase"]["url"]:
            errors.append("official MCP URLs differ between runtimes")
        if claude["mcpServers"]["metabase"].get("type") != "http":
            errors.append("official Claude MCP must use HTTP")
        for path in (root / ".codex/agents").glob("*.toml"):
            data = tomllib.loads(path.read_text(encoding="utf-8"))
            if not all(data.get(key) for key in ("name", "description", "developer_instructions")):
                errors.append(f"invalid Codex agent: {path.name}")
        json.loads((root / ".claude/settings.json").read_text(encoding="utf-8"))
    except (OSError, ValueError, KeyError) as error:
        errors.append(f"configuration: {error}")
    pins = []
    for relative, pattern in [
        (".github/workflows/build.yaml", r"MB_BUILD_REF: (v[\d.]+)"),
        (".github/workflows/release-please.yml", r"MB_BUILD_REF: (v[\d.]+)"),
        ("docker-compose.yaml", r"image: metabase/metabase:(v[\d.]+)"),
        ("docker-compose.compat.yaml", r"metabase/metabase:(v[\d.]+)"),
    ]:
        match = re.search(pattern, (root / relative).read_text(encoding="utf-8"))
        pins.append(match.group(1) if match else None)
    if None in pins or len(set(pins)) != 1:
        errors.append(f"Metabase build/release/demo/compat pins differ: {pins}")
    return errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    problems = check(write=args.write)
    for problem in problems:
        print(problem)
    print(f"Agent assets: {len(WORKFLOWS)} workflows, {'FAIL' if problems else 'OK'}")
    sys.exit(bool(problems))
