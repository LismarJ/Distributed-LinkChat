"""The guard that keeps phase 2 affordable.

ADR-0013 says all application logic lives in `linkchat.core` and that package
never imports Ray, so that phase 2 replaces one package instead of rewriting
the system. A rule nobody checks is a rule that lasts about a week, so this
test fails the build instead.

It parses the source rather than importing it, which means it works even when
Ray is not installed, and it cannot be defeated by an import inside a function
or a `TYPE_CHECKING` block.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

CORE = Path(__file__).resolve().parent.parent / "src" / "linkchat" / "core"

FORBIDDEN_ROOTS = {"ray"}

DYNAMIC_IMPORT = re.compile(
    r"""import_module\s*\(\s*["'](?P<name>[\w.]+)["']""", re.VERBOSE
)


def core_sources() -> list[Path]:
    return sorted(CORE.rglob("*.py"))


def test_core_directory_exists() -> None:
    """Guard the guard: a renamed package would make this test vacuously pass."""
    assert CORE.is_dir(), f"{CORE} is missing; update this test if core moved"
    assert core_sources(), f"no Python files under {CORE}"


def test_core_does_not_import_ray() -> None:
    offenders: list[str] = []

    for path in core_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in FORBIDDEN_ROOTS:
                        offenders.append(f"{path.name}:{node.lineno} import {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.split(".")[0] in FORBIDDEN_ROOTS:
                    offenders.append(f"{path.name}:{node.lineno} from {node.module}")

    assert not offenders, (
        "linkchat.core must not import Ray (ADR-0013). Found:\n  "
        + "\n  ".join(offenders)
        + "\nPut the Ray-facing code in linkchat.ray_runtime and reach it "
        "through a port in linkchat/core/ports.py."
    )


def test_core_does_not_import_ray_dynamically() -> None:
    """Catch the obvious way around an import statement."""
    offenders: list[str] = []
    for path in core_sources():
        for number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            match = DYNAMIC_IMPORT.search(line)
            if match and match.group("name").split(".")[0] in FORBIDDEN_ROOTS:
                offenders.append(f"{path.name}:{number}")
    assert not offenders, (
        "linkchat.core must not reach Ray through importlib either. Found:\n  "
        + "\n  ".join(offenders)
    )


def test_ports_declares_the_agreed_interface() -> None:
    """The frozen names, so that renaming one is a deliberate act.

    Gate G0 froze these on 2026-11-12. Both work tracks are written against
    them, so a rename has to be agreed rather than discovered.
    """
    from linkchat.core import ports

    for name in (
        "RelayGateway",
        "RelayDirectory",
        "RelayPeers",
        "Clock",
        "Record",
        "Tombstone",
        "PullResult",
        "RelayStatus",
        "RelayState",
        "RelayError",
        "RelayUnreachable",
        "RelayGone",
        "RelaySyncing",
        "MailboxFull",
        "CredentialRejected",
    ):
        assert hasattr(ports, name), f"ports.{name} disappeared; see gate G0"
