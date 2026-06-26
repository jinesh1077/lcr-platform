#!/usr/bin/env bash
# Regenerate docs/thorough-test-report.md by running all benchmark suites and merging output.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-$REPO_ROOT/docs/thorough-test-report.md}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cd "$REPO_ROOT"
chmod +x scripts/benchmark/*.sh

echo "=== Generating comprehensive metrics report ==="
echo "Output: $OUT"
echo

run() {
  local name=$1 script=$2
  echo ">> $name"
  "$script" "$TMP/${name}.md" || echo "WARN: $name failed (section may be stale)" >&2
}

run "01-thorough"       ./scripts/benchmark/thorough-test.sh
run "02-data-driven"    ./scripts/benchmark/data-driven-test.sh
run "03-platform"       ./scripts/benchmark/platform-metrics.sh
run "04-scenario"       ./scripts/benchmark/scenario-metrics.sh
run "05-capacity"       ./scripts/benchmark/capacity-study.sh
run "06-high-traffic"   ./scripts/benchmark/high-traffic-study.sh

python3 <<PY
from datetime import datetime, timezone
from pathlib import Path

out = Path("$OUT")
tmp = Path("$TMP")
sections = sorted(tmp.glob("*.md"))

header = f"""# LCR Platform — Comprehensive Metrics Report

**Generated:** {datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
**Environment:** Docker Compose (local)

This report is assembled from all benchmark suites. For a curated static snapshot with full numbers, see the committed version in git.

**Reproduce:** \`make report\`

---

"""

body = []
for path in sections:
    text = path.read_text()
    # Drop duplicate top-level title from each section
    lines = text.splitlines()
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    while lines and not lines[0].strip():
        lines = lines[1:]
    body.append(f"<!-- source: {path.name} -->\n")
    body.append("\n".join(lines))
    body.append("\n\n---\n\n")

footer = """## Individual benchmark commands

| Command | Section source |
|---------|----------------|
| \`make thorough-test\` | Integration correctness |
| \`make data-driven-test\` | E.164 & traffic profile |
| \`make platform-metrics\` | Invariants & latency |
| \`make scenario-metrics\` | Operational scenarios |
| \`make capacity-study\` | Throughput & CPU ROI |
| \`make high-traffic-study\` | 150k+ load test |
"""

out.write_text(header + "".join(body) + footer)
print(f"Wrote {out} ({len(sections)} sections)")
PY

echo "Done: $OUT"
