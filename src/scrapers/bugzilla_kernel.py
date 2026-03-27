"""
Linux Bugzilla scraper
======================

The original kernel Bugzilla scraper targeted https://bugzilla.kernel.org/,
but that site is now fronted by an Anubis proof-of-work challenge that blocks
plain HTTP clients. This scraper switches to bugzilla.suse.com, which exposes
an accessible REST API with recent Linux distro/kernel bugs and comments.

Only resolved/fixed bugs are emitted, and each document requires enough
problem detail plus a resolution comment so the resulting RAG corpus remains
useful for troubleshooting.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator

import requests

import sys
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.schema import LinuxLynxDoc, extract_component, extract_distro, extract_kernel

log = logging.getLogger(__name__)

BASE = "https://bugzilla.suse.com"
REST_BASE = f"{BASE}/rest"
SEARCH_URL = f"{REST_BASE}/bug"

PAGE_LIMIT = 50
MAX_BUGS = 200
MAX_WORKERS = 8
PAGE_DELAY_SECONDS = 0.25

PRODUCTS = [
    "openSUSE Tumbleweed",
    "openSUSE Distribution",
    "SUSE Linux Enterprise Server 15-SP6",
    "SUSE Linux Enterprise Server 15-SP5",
    "SUSE Linux Enterprise Desktop 15-SP6",
    "SUSE Linux Enterprise Desktop 15-SP5",
]

SEARCH_KEYWORDS = [
    "kernel panic",
    "systemd",
    "network",
    "ext4",
    "btrfs",
    "grub",
    "selinux",
    "permission denied",
    "boot failure",
    "kdump",
    "dracut",
    "ssh",
]

HEADERS = {
    "User-Agent": (
        "LinuxLynx-DataCollector/2.0 "
        "(research dataset; contact: dataset@linuxlynx.dev)"
    )
}

_FIX_RE = re.compile(
    r"(?:fix(?:ed)?|resolved|works now|updated kernel|kernel update|closed|"
    r"patch(?:ed)?|commit|backport)",
    re.IGNORECASE,
)
_PATCH_RE = re.compile(
    r"https?://\S+|(?:commit|patch)[:\s]+[0-9a-f]{7,40}",
    re.IGNORECASE,
)
_VERSION_RE = re.compile(r"\b(\d+\.\d+(?:\.\d+)?(?:-[\w.]+)?)\b")
_LOG_LINE_RE = re.compile(
    r"^\[[\s\d.]+\]|^BUG:|^WARNING:|^Oops|^Call Trace|^RIP:|"
    r"^Kernel panic|^systemd\[[0-9]+\]:|^dracut:|^grub",
    re.IGNORECASE,
)


def _get(url: str, params: dict | None = None, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt + 1, retries, exc)
            time.sleep(PAGE_DELAY_SECONDS * (attempt + 1))
    return None


def _clean_text(text: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", (text or "").strip())


def _map_component_to_domain(component: str, product: str, all_text: str) -> str:
    combined = f"{component} {product} {all_text}".lower()
    mapping = {
        "network": "networking",
        "wicked": "networking",
        "kernel": "kernel",
        "systemd": "systemd",
        "selinux": "security",
        "apparmor": "security",
        "ext4": "filesystem",
        "btrfs": "filesystem",
        "xfs": "filesystem",
        "grub": "boot",
        "dracut": "boot",
        "initrd": "boot",
        "kdump": "kernel",
        "package": "package",
        "zypper": "package",
        "ssh": "security",
        "storage": "storage",
    }
    for needle, domain in mapping.items():
        if needle in combined:
            return domain
    return "other"


def _map_failure_type(summary: str, comments_text: str) -> str:
    combined = f"{summary} {comments_text}".lower()
    if "panic" in combined:
        return "kernel panic"
    if "segfault" in combined or "segmentation fault" in combined:
        return "segfault"
    if "permission denied" in combined or "selinux" in combined or "apparmor" in combined:
        return "permission"
    if "timeout" in combined:
        return "network timeout"
    if "corrupt" in combined:
        return "disk corruption"
    if "depend" in combined or "missing package" in combined:
        return "dependency"
    if "config" in combined or "misconfigur" in combined:
        return "config error"
    return "other"


def _infer_distro(product: str, op_sys: str, text: str) -> str:
    if product:
        return product
    if op_sys and op_sys != "All":
        return op_sys
    return extract_distro(text)


def _extract_version_scope(version: str, text: str) -> str:
    versions = _VERSION_RE.findall(text)
    if version and version.lower() not in {"all", "current", "unknown", "unspecified"}:
        versions.insert(0, version)
    unique = []
    for item in versions:
        if item not in unique:
            unique.append(item)
    return ", ".join(unique[:8]) if unique else "unknown"


def _pick_raw_logs(comments: list[dict]) -> str:
    candidates: list[str] = []
    for comment in comments[:6]:
        text = _clean_text(comment.get("text", ""))
        if not text:
            continue

        log_lines = [line for line in text.splitlines() if _LOG_LINE_RE.search(line)]
        if log_lines:
            candidates.append("\n".join(log_lines[:80]).strip())

        if len(text) >= 120:
            candidates.append(text[:2000])

    for candidate in candidates:
        if len(candidate.strip()) >= 50:
            return candidate[:2000].strip()
    return ""


def _pick_fix_comment(comments: list[dict]) -> str:
    for comment in reversed(comments):
        text = _clean_text(comment.get("text", ""))
        if not text:
            continue
        if _PATCH_RE.search(text) or _FIX_RE.search(text):
            return text[:1200]
    return _clean_text(comments[-1].get("text", ""))[:1200] if comments else ""


def _fetch_comments(bug_id: int) -> list[dict]:
    data = _get(f"{REST_BASE}/bug/{bug_id}/comment")
    if not data:
        return []
    return data.get("bugs", {}).get(str(bug_id), {}).get("comments", [])


def _build_doc_from_bug(bug: dict) -> LinuxLynxDoc | None:
    bug_id = bug["id"]
    comments = _fetch_comments(bug_id)
    if not comments:
        return None

    summary = bug.get("summary", "")
    product = bug.get("product", "")
    version = bug.get("version", "unknown")
    component = bug.get("component", "unknown")
    op_sys = bug.get("op_sys", "")
    bug_url = f"{BASE}/show_bug.cgi?id={bug_id}"

    raw_logs = _pick_raw_logs(comments)
    if len(raw_logs) < 50:
        return None

    fix_comment = _pick_fix_comment(comments)
    if len(fix_comment) < 20:
        return None

    all_text = "\n\n".join(_clean_text(comment.get("text", "")) for comment in comments if comment.get("text"))
    middle_comments = comments[1:-1] if len(comments) > 2 else []
    debug_steps = "\n---\n".join(
        _clean_text(comment.get("text", ""))[:350]
        for comment in middle_comments[:4]
        if comment.get("text")
    ).strip()

    root_cause = "unknown"
    for pattern in [
        r"(?:root cause|the (?:problem|issue|bug) (?:is|was))[:\s]+(.{20,350})",
        r"(?:caused by|triggered by|because)[:\s]+(.{20,350})",
        r"(?:regression since|introduced by)[:\s]+(.{20,250})",
    ]:
        match = re.search(pattern, all_text, re.IGNORECASE | re.DOTALL)
        if match:
            root_cause = match.group(1).strip()[:400]
            break

    reasoning = ""
    for pattern in [
        r"(?:this fix|the fix|solution)[:\s]+(.{20,300})",
        r"because[:\s]+(.{20,250})",
    ]:
        match = re.search(pattern, fix_comment, re.IGNORECASE | re.DOTALL)
        if match:
            reasoning = match.group(1).strip()[:400]
            break

    distro = _infer_distro(product, op_sys, all_text)
    kernel = extract_kernel(f"{summary}\n{all_text}")
    component_name = extract_component(all_text, fallback=component)

    doc = LinuxLynxDoc.build(
        doc_id=f"bugzilla_{bug_id}",
        source="bugzilla",
        domain=_map_component_to_domain(component, product, all_text),
        failure_type=_map_failure_type(summary, all_text),
        distro=distro,
        kernel=kernel,
        component=component_name,
        problem=summary[:220],
        raw_logs=raw_logs,
        debug_steps=debug_steps[:1000],
        root_cause=root_cause,
        solution=fix_comment[:900],
        reasoning=reasoning,
        version_scope=_extract_version_scope(version, all_text),
        confidence="high",
        link=bug_url,
    )

    errors = doc.validate()
    if errors:
        log.debug("Bug %s validation errors: %s", bug_id, errors)
        return None
    return doc


def _search_bugs(max_candidates: int, keywords: list[str] | None = None) -> list[dict]:
    search_terms = keywords or SEARCH_KEYWORDS
    quicksearch = " OR ".join(f'"{term}"' if " " in term else term for term in search_terms)
    offset = 0
    seen_bug_ids: set[int] = set()
    bugs: list[dict] = []

    while len(bugs) < max_candidates:
        page_size = min(PAGE_LIMIT, max_candidates - len(bugs))
        params = {
            "status": "RESOLVED",
            "resolution": "FIXED",
            "product": PRODUCTS,
            "quicksearch": quicksearch,
            "include_fields": (
                "id,summary,component,version,status,resolution,creator,"
                "last_change_time,product,op_sys"
            ),
            "order": "changeddate DESC",
            "limit": page_size,
            "offset": offset,
        }

        data = _get(SEARCH_URL, params=params)
        if not data:
            break

        page_bugs = data.get("bugs", [])
        if not page_bugs:
            break

        for bug in page_bugs:
            bug_id = bug["id"]
            if bug_id in seen_bug_ids:
                continue
            seen_bug_ids.add(bug_id)
            bugs.append(bug)
            if len(bugs) >= max_candidates:
                break

        offset += len(page_bugs)
        if len(page_bugs) < page_size:
            break
        time.sleep(PAGE_DELAY_SECONDS)

    return bugs


def scrape(
    max_bugs: int = MAX_BUGS,
    keywords: list[str] | None = None,
) -> Generator[LinuxLynxDoc, None, None]:
    """
    Yield LinuxLynxDoc objects from recent resolved Linux Bugzilla issues.
    """
    candidate_target = max(12, max_bugs * 2)
    candidates = _search_bugs(candidate_target, keywords=keywords)
    if not candidates:
        return

    yielded = 0
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates))) as executor:
        futures = {
            executor.submit(_build_doc_from_bug, bug): bug["id"]
            for bug in candidates
        }
        for future in as_completed(futures):
            doc = future.result()
            if not doc:
                continue
            yielded += 1
            yield doc
            if yielded >= max_bugs:
                return


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator

    deduper = Deduplicator()
    output = sys.argv[1] if len(sys.argv) > 1 else "bugzilla_kernel.jsonl"
    count = 0

    with open(output, "w", encoding="utf-8") as handle:
        for doc in scrape(max_bugs=5):
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if deduper.is_duplicate(content_to_hash):
                print(f"  [-] DUP: {doc.doc_id}", file=sys.stderr)
                continue
            handle.write(doc.to_jsonl() + "\n")
            count += 1
            print(f"  [{count}] NEW: {doc.doc_id}: {doc.problem[:70]}", file=sys.stderr)

    deduper._save_hashes()
    print(f"\nWrote {count} documents to {output}", file=sys.stderr)
