"""
Kernel Bugzilla scraper  —  https://bugzilla.kernel.org/
Targets confirmed-fixed bugs (RESOLVED FIXED) with logs attached.

Filtering:
  - Status: RESOLVED, VERIFIED
  - Resolution: FIXED
  - Has at least one comment with a patch URL, commit hash, or fix description
  - raw_logs >= 50 chars
"""

from __future__ import annotations
import re
import time
import logging
from typing import Generator

import requests
import cloudscraper

_scraper = cloudscraper.create_scraper()
from bs4 import BeautifulSoup

# project-local
import sys, os
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from src.schema import (
    LinuxLynxDoc, extract_distro, extract_kernel,
    extract_component, classify_risk,
)

log = logging.getLogger(__name__)

BASE      = "https://bugzilla.kernel.org"
REST_BASE = f"{BASE}/rest"

# Kernel Bugzilla REST API v1
SEARCH_URL = f"{REST_BASE}/bug"

# How many bugs to fetch per API page
PAGE_LIMIT = 50

# Maximum bugs to scrape per run (set None for unlimited)
MAX_BUGS = 200

# Rate limiting — be polite
DELAY_SECONDS = 1.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

_COMMIT_RE = re.compile(
    r"(?:commit|fix(?:ed)?\s+in|patch)[:\s]+([0-9a-f]{7,40})", re.IGNORECASE
)
_PATCH_RE = re.compile(r"https?://(?:lore\.kernel\.org|patchwork\.|git\.kernel\.org)\S+")
_VERSION_RE = re.compile(r"\b(\d+\.\d+(?:\.\d+)?(?:-rc\d+)?)\b")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get(url: str, params: dict | None = None, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = _scraper.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt+1, retries, e)
            time.sleep(DELAY_SECONDS * (attempt + 1))
    return None


def _strip_html(html: str) -> str:
    return BeautifulSoup(html, "html.parser").get_text(separator="\n")


def _extract_code_blocks(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    blocks = []
    for tag in soup.find_all(["pre", "code"]):
        txt = tag.get_text()
        if len(txt.strip()) > 20:
            blocks.append(txt.strip())
    return "\n---\n".join(blocks)


def _map_component_to_domain(component: str) -> str:
    _MAP = {
        "networking": "networking",
        "drivers/net": "networking",
        "fs": "filesystem",
        "mm": "memory",
        "kernel": "kernel",
        "arch": "kernel",
        "security": "security",
        "block": "storage",
        "drivers/block": "storage",
        "virt": "virtualization",
        "drivers/gpu": "hardware",
        "init": "boot",
        "systemd": "systemd",
    }
    c = component.lower()
    for key, domain in _MAP.items():
        if key in c:
            return domain
    return "kernel"   # default for bugzilla.kernel.org


def _map_failure_type(summary: str, comments_text: str) -> str:
    combined = (summary + " " + comments_text).lower()
    if "panic" in combined:
        return "kernel panic"
    if "segfault" in combined or "segmentation fault" in combined:
        return "segfault"
    if "permission denied" in combined:
        return "permission"
    if "timeout" in combined:
        return "network timeout"
    if "corrupt" in combined:
        return "disk corruption"
    if "depend" in combined or "missing module" in combined:
        return "dependency"
    if "config" in combined or "misconfigur" in combined:
        return "config error"
    return "other"


def _extract_version_scope(comments_text: str) -> str:
    versions = _VERSION_RE.findall(comments_text)
    if versions:
        return ", ".join(sorted(set(versions))[:6])
    return "unknown"


# ── Bug detail fetcher ────────────────────────────────────────────────────────

def _fetch_bug_detail(bug_id: int) -> LinuxLynxDoc | None:
    """Fetch a single bug and its comments; return a LinuxLynxDoc or None."""
    data = _get(f"{REST_BASE}/bug/{bug_id}")
    if not data or "bugs" not in data or not data["bugs"]:
        return None
    bug = data["bugs"][0]

    # Only resolved+fixed
    if bug.get("status") not in ("RESOLVED", "VERIFIED"):
        return None
    if bug.get("resolution") != "FIXED":
        return None

    summary     = bug.get("summary", "")
    component   = bug.get("component", "unknown")
    product     = bug.get("product", "")
    creator     = bug.get("creator", "")
    version     = bug.get("version", "unknown")
    bug_url     = f"{BASE}/show_bug.cgi?id={bug_id}"

    # Fetch comments
    cdata = _get(f"{REST_BASE}/bug/{bug_id}/comment")
    if not cdata:
        return None

    comments = cdata.get("bugs", {}).get(str(bug_id), {}).get("comments", [])
    if not comments:
        return None

    # Bugzilla REST API returns comment text as plain text, not HTML.
    # _extract_code_blocks (which looks for <pre>/<code> tags) will always
    # return "" on plain text.  Use first_text directly as raw_logs.
    first_text = _strip_html(comments[0].get("text", ""))
    raw_logs = first_text

    if len(raw_logs.strip()) < 10:
        # Try second comment
        if len(comments) > 1:
            raw_logs = _strip_html(comments[1].get("text", ""))
        if len(raw_logs.strip()) < 10:
            log.debug("Bug %d skipped: raw_logs too short", bug_id)
            return None

    # --- Find the resolution comment (last comment or one mentioning fix)
    all_text   = "\n\n".join(_strip_html(c.get("text", "")) for c in comments)
    fix_comment = ""
    for c in reversed(comments):
        t = _strip_html(c.get("text", ""))
        if _COMMIT_RE.search(t) or _PATCH_RE.search(t) or "fixed" in t.lower():
            fix_comment = t
            break

    if not fix_comment:
        fix_comment = _strip_html(comments[-1].get("text", ""))

    # --- Build fields
    distro    = extract_distro(all_text)
    kernel    = extract_kernel(all_text)
    comp_name = extract_component(all_text, fallback=component)

    # Infer debug_steps from middle comments (maintainer back-and-forth)
    debug_comments = comments[1:-1] if len(comments) > 2 else []
    debug_steps = "\n".join(
        _strip_html(c.get("text", ""))[:300]
        for c in debug_comments[:5]
    ).strip()

    # Root cause: look for explicit diagnosis language
    root_cause = "unknown"
    for pattern in [
        r"(?:root cause|the (?:bug|issue|problem) (?:is|was)|cause[d]? by)[:\s]+(.{20,300})",
        r"(?:introduced by|regression (?:from|since))[:\s]+(.{20,200})",
    ]:
        m = re.search(pattern, all_text, re.IGNORECASE | re.DOTALL)
        if m:
            root_cause = m.group(1).strip()[:400]
            break

    # Reasoning: pull explanation from fix comment
    reasoning = ""
    for pattern in [
        r"(?:this fix|the fix|we (?:fix|resolve)|solution)[:\s]+(.{20,400})",
        r"(?:because|the reason)[:\s]+(.{20,300})",
    ]:
        m = re.search(pattern, fix_comment, re.IGNORECASE | re.DOTALL)
        if m:
            reasoning = m.group(1).strip()[:500]
            break

    version_scope = _extract_version_scope(all_text)
    if version != "unspecified":
        version_scope = version if version_scope == "unknown" else version_scope

    commit_match = _COMMIT_RE.search(all_text)
    solution = fix_comment[:800].strip()
    if commit_match:
        solution = f"Commit {commit_match.group(1)}\n\n{solution}"

    doc = LinuxLynxDoc.build(
        doc_id     = f"bugzilla_{bug_id}",
        source     = "bugzilla",
        domain     = _map_component_to_domain(component),
        failure_type = _map_failure_type(summary, all_text),
        distro     = distro,
        kernel     = kernel,
        component  = comp_name,
        problem    = summary,
        raw_logs   = raw_logs[:2000].strip(),
        debug_steps = debug_steps[:1000],
        root_cause = root_cause,
        solution   = solution,
        reasoning  = reasoning,
        version_scope = version_scope,
        confidence = "high",   # bugzilla RESOLVED FIXED = high
        link       = bug_url,
    )

    errs = doc.validate()
    if errs:
        log.debug("Bug %d validation errors: %s", bug_id, errs)
        return None

    return doc


# ── Main search loop ──────────────────────────────────────────────────────────

def scrape(
    max_bugs: int = MAX_BUGS,
    keywords: list[str] | None = None,
) -> Generator[LinuxLynxDoc, None, None]:
    """
    Yield LinuxLynxDoc objects for resolved+fixed kernel bugs.

    Args:
        max_bugs:  Maximum number of bugs to attempt (across all pages).
        keywords:  If supplied, restrict to bugs whose summary matches.
    """
    offset = 0
    fetched = 0

    while fetched < max_bugs:
        params: dict = {
            "bug_status": "RESOLVED",
            "resolution": "FIXED",
            "limit":      PAGE_LIMIT,
            "offset":     offset,
            "include_fields": "id,summary,component,version,status,resolution,creator",
        }
        if keywords:
            params["summary"] = " ".join(keywords)

        data = _get(SEARCH_URL, params=params)
        if not data:
            break

        bugs = data.get("bugs", [])
        if not bugs:
            log.info("No more bugs at offset %d", offset)
            break

        for bug_meta in bugs:
            if fetched >= max_bugs:
                return
            bug_id = bug_meta["id"]
            log.info("Processing bug %d (%d/%d)", bug_id, fetched + 1, max_bugs)
            time.sleep(DELAY_SECONDS)

            doc = _fetch_bug_detail(bug_id)
            if doc:
                fetched += 1
                yield doc
            else:
                log.debug("Bug %d skipped", bug_id)

        offset += PAGE_LIMIT

        # If we got fewer than PAGE_LIMIT, we've exhausted results
        if len(bugs) < PAGE_LIMIT:
            break


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator
    deduper = Deduplicator()
    
    output = sys.argv[1] if len(sys.argv) > 1 else "bugzilla_kernel.jsonl"
    count = 0
    with open(output, "w") as fh:
        for doc in scrape(max_bugs=5):
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if not deduper.is_duplicate(content_to_hash):
                fh.write(doc.to_jsonl() + "\n")
                count += 1
                print(f"  [{count}] NEW: {doc.doc_id}: {doc.problem[:60]}", file=sys.stderr)
            else:
                print(f"  [-] DUP: {doc.doc_id}", file=sys.stderr)
        # save hashes
        deduper._save_hashes()
    print(f"\nWrote {count} documents to {output}", file=sys.stderr)
