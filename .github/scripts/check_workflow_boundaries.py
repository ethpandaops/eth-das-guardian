#!/usr/bin/env python3
"""Regression checks for DEV-image workflow boundaries."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
CANDIDATE = WORKFLOWS / "build-dev-candidate.yml"
PUBLISHER = WORKFLOWS / "build-dev.yml"


def require(condition: bool, message: str) -> None:
    if not condition:
        print(f"workflow boundary check failed: {message}", file=sys.stderr)
        raise SystemExit(1)


workflow_text = {
    path.name: path.read_text()
    for path in sorted(WORKFLOWS.glob("*.y*ml"))
}
candidate = CANDIDATE.read_text()
publisher = PUBLISHER.read_text()

require(
    all("pull_request_target:" not in text for text in workflow_text.values()),
    "pull_request_target must not be used",
)
require("pull_request:" in candidate, "candidate builds must use pull_request")
require("contents: read" in candidate, "candidate workflow must remain read-only")
require(": write" not in candidate, "candidate workflow must not request write permissions")
require(
    "build-docker-image" in candidate,
    "candidate builds must remain gated by the maintainer label",
)
require("secrets." not in candidate, "candidate workflow must not receive secrets")
require(
    "persist-credentials: false" in candidate,
    "candidate checkout must not persist credentials",
)
require("docker/login-action" not in candidate, "candidate workflow must not log in")
require(
    not re.search(r"^\s*docker\s+(?:push|manifest\s+push)\b", candidate, re.MULTILINE),
    "candidate workflow must not publish images",
)

require("issue_comment:" in publisher, "publisher must use the default-branch comment workflow")
require("workflow_dispatch:" not in publisher, "publisher must not run from a selected branch")
require("pull_request:" not in publisher, "publisher must not run on pull requests")
require("actions/checkout" not in publisher, "publisher must not check out PR code")
require("docker build" not in publisher, "publisher must not build PR code")
require(
    'collaborators/${COMMENTER}/permission' in publisher,
    "publisher must verify commenter permission",
)
require(
    'current_sha" != "$REVIEWED_SHA' in publisher,
    "publisher must reject a stale reviewed SHA",
)
require(
    'select(.commit_id == $sha)' in publisher
    and '== "APPROVED"' in publisher,
    "publisher must require approval for the exact reviewed SHA",
)
require(
    'sha256sum --check "dev-image-${platform}.tar.sha256"' in publisher,
    "publisher must verify downloaded candidate archives",
)
require(
    publisher.count("secrets.DOCKERHUB_") == 2,
    "only the publisher may consume both Docker Hub credentials",
)
for name, text in workflow_text.items():
    if name != PUBLISHER.name:
        require(
            "secrets.DOCKERHUB_" not in text or name in {"_shared-build.yaml", "build-main.yml", "build-release.yml"},
            f"unexpected Docker Hub credential consumer: {name}",
        )

unpinned = []
for name, text in workflow_text.items():
    for line_number, line in enumerate(text.splitlines(), 1):
        match = re.search(r"\buses:\s+([^\s#]+)", line)
        if not match or match.group(1).startswith("./"):
            continue
        reference = match.group(1).rsplit("@", 1)[-1]
        if not re.fullmatch(r"[0-9a-f]{40}", reference):
            unpinned.append(f"{name}:{line_number}: {match.group(1)}")
require(not unpinned, "actions must be pinned by full SHA: " + ", ".join(unpinned))

print("workflow boundaries verified")
