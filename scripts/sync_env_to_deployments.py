#!/usr/bin/env python3
"""Synchronize non-sensitive environment variables from .env.prod to Helm values.yaml."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SENSITIVE_SUBSTRINGS = ("PASSWORD", "SECRET", "TOKEN", "KEY")
DEFAULT_ENV_FILE = ".env.prod"
DEFAULT_VALUES_FILE = "deployments/helm/values.yaml"


def configure_utf8_stdio() -> None:
    """Ensure UTF-8 output works cleanly on Windows consoles."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def is_sensitive_key(key: str) -> bool:
    """Return True if key looks like a credential or secret."""
    upper = key.upper()
    return any(marker in upper for marker in SENSITIVE_SUBSTRINGS)


def parse_env_value(raw: str) -> str:
    """Unquote and unescape a single .env value."""
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ('"', "'"):
        quote = raw[0]
        inner = raw[1:-1]
        if quote == '"':
            inner = (
                inner.replace(r"\"", '"')
                .replace(r"\n", "\n")
                .replace(r"\r", "\r")
                .replace(r"\t", "\t")
                .replace(r"\\", "\\")
            )
        return inner

    comment_idx = raw.find(" #")
    if comment_idx >= 0:
        raw = raw[:comment_idx]
    return raw.strip()


def load_env_file(path: Path) -> dict[str, str]:
    """Parse a .env file into an ordered dictionary of non-sensitive key/value pairs."""
    if not path.is_file():
        raise FileNotFoundError(f"Environment file not found: {path}")

    entries: dict[str, str] = {}
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    for line_num, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            raise ValueError(f"Invalid syntax in {path} at line {line_num}: missing '='")

        key, raw_val = line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid syntax in {path} at line {line_num}: empty key")

        if is_sensitive_key(key):
            continue

        entries[key] = parse_env_value(raw_val.strip())

    return entries


def parse_yaml_scalar(raw: str) -> str:
    """Parse a simple YAML scalar value from an env block line."""
    val = raw.strip()
    if len(val) >= 2 and val[0] == '"' and val[-1] == '"':
        inner = val[1:-1]
        return (
            inner.replace(r"\"", '"')
            .replace(r"\n", "\n")
            .replace(r"\r", "\r")
            .replace(r"\t", "\t")
            .replace(r"\\", "\\")
        )
    if len(val) >= 2 and val[0] == "'" and val[-1] == "'":
        return val[1:-1].replace("''", "'")
    comment_idx = val.find(" #")
    if comment_idx >= 0:
        val = val[:comment_idx].strip()
    return val


def format_yaml_double_quoted(val: str) -> str:
    """Format a string as a safe double-quoted YAML scalar."""
    escaped = (
        val.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def find_env_block_bounds(lines: list[str]) -> tuple[int, int]:
    """Return (start_idx, end_idx) of the top-level `env:` block in values.yaml."""
    start_idx = -1
    for idx, line in enumerate(lines):
        if re.match(r"^env:\s*(#.*)?$", line):
            start_idx = idx
            break
    if start_idx < 0:
        raise ValueError("Top-level 'env:' block not found in values.yaml")

    end_idx = start_idx + 1
    while end_idx < len(lines):
        line = lines[end_idx]
        if line.strip() == "" or line.startswith(" ") or line.startswith("\t") or line.lstrip().startswith("#"):
            # Stop if a top-level comment precedes the next top-level key
            if line and not line[0].isspace() and line.startswith("#"):
                break
            end_idx += 1
        else:
            break

    # Trim trailing blank lines from the block so surrounding spacing stays intact
    while end_idx > start_idx + 1 and lines[end_idx - 1].strip() == "":
        end_idx -= 1

    return start_idx, end_idx


def parse_values_env_block(lines: list[str], start_idx: int, end_idx: int) -> tuple[str | None, dict[str, str]]:
    """Extract secretName (raw line if present) and key/value pairs from values.yaml env block."""
    secret_name_line: str | None = None
    env_map: dict[str, str] = {}

    for line in lines[start_idx + 1 : end_idx]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            continue
        key, raw_val = stripped.split(":", 1)
        key = key.strip()
        if key == "secretName":
            secret_name_line = line
            continue
        env_map[key] = parse_yaml_scalar(raw_val)

    return secret_name_line, env_map


def check_drift(expected: dict[str, str], actual: dict[str, str]) -> list[str]:
    """Return human-readable drift descriptions between .env.prod and values.yaml."""
    diffs: list[str] = []
    for key, exp_val in expected.items():
        if key not in actual:
            diffs.append(f"  + missing in values.yaml: {key}")
        elif actual[key] != exp_val:
            diffs.append(f"  ~ mismatch for {key}: values.yaml={actual[key]!r} vs .env.prod={exp_val!r}")

    for key in actual:
        if key not in expected:
            diffs.append(f"  - extra in values.yaml (not in .env.prod): {key}")

    return diffs


def sync_values_file(values_path: Path, expected: dict[str, str]) -> bool:
    """Update the `env:` block in values.yaml from expected env pairs. Returns True if changed."""
    original_text = values_path.read_text(encoding="utf-8")
    newline = "\r\n" if "\r\n" in original_text else "\n"
    lines = original_text.splitlines()

    start_idx, end_idx = find_env_block_bounds(lines)
    secret_name_line, actual = parse_values_env_block(lines, start_idx, end_idx)

    if not check_drift(expected, actual):
        return False

    new_env_lines = [lines[start_idx]]
    if secret_name_line is not None:
        new_env_lines.append(secret_name_line)
    for key, val in expected.items():
        new_env_lines.append(f"  {key}: {format_yaml_double_quoted(val)}")

    updated_lines = lines[:start_idx] + new_env_lines + lines[end_idx:]
    trailing = newline if original_text.endswith(("\n", "\r\n")) else ""
    values_path.write_text(newline.join(updated_lines) + trailing, encoding="utf-8")
    return True


def main() -> int:
    configure_utf8_stdio()

    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Synchronize and validate .env.prod against deployments/helm/values.yaml"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sync", action="store_true", help="Update deployments/helm/values.yaml from .env.prod")
    mode.add_argument("--check-only", action="store_true", help="Verify zero drift without modifying files")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help="Path to source .env.prod file")
    parser.add_argument("--values-file", default=DEFAULT_VALUES_FILE, help="Path to target Helm values.yaml")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = repo_root / env_path

    values_path = Path(args.values_file)
    if not values_path.is_absolute():
        values_path = repo_root / values_path

    try:
        expected = load_env_file(env_path)
        if not values_path.is_file():
            raise FileNotFoundError(f"Helm values file not found: {values_path}")

        if args.check_only:
            lines = values_path.read_text(encoding="utf-8").splitlines()
            start_idx, end_idx = find_env_block_bounds(lines)
            _, actual = parse_values_env_block(lines, start_idx, end_idx)
            diffs = check_drift(expected, actual)
            if diffs:
                print(
                    "ERROR: Environment drift detected between .env.prod and deployments/helm/values.yaml:",
                    file=sys.stderr,
                )
                for diff in diffs:
                    print(diff, file=sys.stderr)
                print(
                    "Run `python scripts/sync_env_to_deployments.py --sync` to synchronize.",
                    file=sys.stderr,
                )
                return 1
            print("OK: deployments/helm/values.yaml is in sync with .env.prod.")
            return 0

        changed = sync_values_file(values_path, expected)
        if changed:
            print("SYNCED: Updated deployments/helm/values.yaml from .env.prod.")
        else:
            print("OK: deployments/helm/values.yaml already up to date.")
        return 0
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
