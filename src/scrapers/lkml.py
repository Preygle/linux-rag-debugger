"""
LKML / lore.kernel.org mailing list scraper
============================================
Source: https://lore.kernel.org/  (machine-readable Atom feeds + /raw/ endpoints)

Strategy
--------
1. Search lore.kernel.org full-text search (/?q=...) for threads
   containing known failure keywords.
2. For each thread URL, fetch the Atom feed (append /?q=&x=A) to list messages.
3. Fetch each message /raw/ to get plain-text content.
4. Apply validity filters:
   - Thread must contain a reply with a patch diff, commit hash, or "fix"
   - Thread must NOT be [RFC] or [PATCH WIP]
   - Original reporter must confirm fix OR thread marked as closed
5. Map to LinuxLynxDoc schema.

Rate limiting: 1 req/2s — lore.kernel.org is a public archiver.
"""

from __future__ import annotations
import re
import time
import logging
from email import message_from_string
from typing import Generator
from urllib.parse import urljoin, urlencode, quote_plus

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings

# The Atom feeds are XML, but html.parser works fine for extracting links.
# We suppress the generic warning to keep the logs clean.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

import sys, os
# Ensure project root is in path whether run directly or imported
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from src.schema import (
    LinuxLynxDoc, extract_distro, extract_kernel,
    extract_component, classify_risk,
)

log = logging.getLogger(__name__)

BASE  = "https://lore.kernel.org"
LISTS = [
    "linux-kernel",     # LKML proper
    "linux-mm",         # memory management
    "linux-fsdevel",    # filesystem bugs
    "netdev",           # networking
    "linux-block",      # block/storage
    "linux-security-module",
]

FAILURE_KEYWORDS = [
    "kernel panic", "oops", "BUG:", "use-after-free", "null pointer",
    "WARNING:", "WARN_ON", "segfault", "page fault", "hung task",
    "deadlock", "race condition", "memory leak", "call trace",
    "RIP:", "general protection fault",
]

DELAY_SECONDS  = 2.0
MAX_THREADS    = 50  # per list per keyword
MAX_TOTAL_DOCS = 300

HEADERS = {
    "User-Agent": "LinuxLynx-DataCollector/1.0 (research; contact: dataset@linuxlynx.dev)"
}

# ── Patterns ─────────────────────────────────────────────────────────────────

_PATCH_RE    = re.compile(r"^\+{3} |^-{3} |^diff --git|^index [0-9a-f]+\.\.", re.M)
_COMMIT_RE   = re.compile(r"\b([0-9a-f]{12,40})\b")
_FIXED_RE    = re.compile(
    r"(?:fix(?:ed)?|applied|merged|thank[s]?|work[s]? now|confirmed|resolved)",
    re.IGNORECASE,
)
_RFC_RE      = re.compile(r"\[RFC\]|\[PATCH.*WIP\]|\[PATCH v\d+ WIP\]", re.IGNORECASE)
_VERSION_RE  = re.compile(r"\b(\d+\.\d+(?:\.\d+)?(?:-rc\d+|-stable)?)\b")
_SUBJECT_JUNK = re.compile(r"\[PATCH[^\]]*\]\s*|\[RFC[^\]]*\]\s*|Re:\s*", re.IGNORECASE)

_DOMAIN_HINTS = {
    "linux-mm":     "memory",
    "linux-fsdevel":"filesystem",
    "netdev":       "networking",
    "linux-block":  "storage",
    "linux-security-module": "security",
    "linux-kernel": "kernel",
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning("GET %s failed (attempt %d): %s", url, attempt + 1, e)
            time.sleep(DELAY_SECONDS * (attempt + 1))
    return None


def _get_text(url: str) -> str:
    r = _get(url)
    return r.text if r else ""


# ── Thread discovery ──────────────────────────────────────────────────────────

def _search_list(list_name: str, keyword: str, max_threads: int) -> list[str]:
    """
    Return thread URLs for `list_name` matching `keyword`.
    Uses lore.kernel.org search endpoint.
    """
    q    = quote_plus(keyword)
    # Do NOT use &x=A here — that returns an Atom/XML feed where links are
    # in <link> elements, not <a> tags.  The plain HTML search results page
    # has the <a href> links our parser expects.
    url  = f"{BASE}/{list_name}/?q={q}"
    html = _get_text(url)
    if not html:
        return []

    soup  = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Thread links look like /<list>/T/#<msg-id> or /<list>/<msg-id>/
        if href.startswith(f"/{list_name}/") and "@" in href:
            full = urljoin(BASE, href)
            # Normalise to thread root
            thread = re.sub(r"#.*$", "", full).rstrip("/")
            if thread not in links:
                links.append(thread)
        if len(links) >= max_threads:
            break

    return links


# ── Message fetching ──────────────────────────────────────────────────────────

def _fetch_thread_messages(thread_url: str) -> list[dict]:
    """
    Fetch all messages in a thread.
    Uses the T/ (thread) endpoint and follows individual /raw/ links.
    """
    html = _get_text(thread_url + "/T/")
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    messages = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Individual message: href ends with / and contains @
        if href.endswith("/") and "@" in href and not href.endswith("/T/"):
            msg_url = urljoin(thread_url + "/T/", href)
            raw_url = msg_url.rstrip("/") + "/raw"
            raw     = _get_text(raw_url)
            if raw:
                messages.append({
                    "url": msg_url,
                    "raw": raw,
                })
            time.sleep(DELAY_SECONDS / 2)

    return messages


# ── Thread validation ─────────────────────────────────────────────────────────

def _is_valid_thread(messages: list[dict]) -> bool:
    """
    A thread is valid if:
    - NOT [RFC] or [PATCH WIP] in subject
    - At least one message contains a patch diff OR commit hash
    - Thread has at least 2 messages (report + response)
    - Last few messages indicate resolution (fix confirmed / applied)
    """
    if not messages:
        return False

    # Check first message subject for RFC/WIP
    first_raw = messages[0]["raw"]
    subject_m = re.search(r"^Subject:\s*(.+)$", first_raw, re.MULTILINE)
    subject   = subject_m.group(1) if subject_m else ""
    if _RFC_RE.search(subject):
        return False

    all_text = "\n".join(m["raw"] for m in messages)

    # Must have a patch or commit reference
    has_patch  = bool(_PATCH_RE.search(all_text))
    has_commit = bool(_COMMIT_RE.search(all_text))
    if not (has_patch or has_commit):
        return False

    # Relaxed to 2 — a report + fix reply is sufficient
    if len(messages) < 2:
        return False

    # _FIXED_RE was defined but never used before — check that the thread
    # actually has a resolution signal in its later messages.
    last_msgs = "\n".join(m["raw"] for m in messages[-4:])
    if not _FIXED_RE.search(last_msgs) and not has_patch:
        return False

    return True


# ── Document builder ──────────────────────────────────────────────────────────

def _build_doc(
    thread_url: str,
    messages: list[dict],
    list_name: str,
) -> LinuxLynxDoc | None:
    if not messages:
        return None

    first_raw   = messages[0]["raw"]
    all_text    = "\n".join(m["raw"] for m in messages)
    last_msgs   = "\n".join(m["raw"] for m in messages[-5:])

    # Extract email headers from first message
    try:
        msg     = message_from_string(first_raw)
        subject = msg.get("Subject", "").strip()
        subject = _SUBJECT_JUNK.sub("", subject).strip()
    except Exception:
        subject = ""

    # raw_logs: extract verbatim log blocks (lines starting with typical kernel log patterns)
    log_lines = []
    in_block  = False
    for line in first_raw.splitlines():
        if re.match(r"^\[[\s\d.]+\]|^BUG:|^WARNING:|^Oops|^Call Trace|^RIP:|^---", line):
            in_block = True
        if in_block:
            log_lines.append(line)
            if line.strip() == "" and len(log_lines) > 5:
                in_block = False

    raw_logs = "\n".join(log_lines[:60]).strip()
    if len(raw_logs) < 50:
        # Fallback 1: indented/code-like lines
        raw_logs = "\n".join(
            line for line in first_raw.splitlines()
            if line.startswith("  ") or line.startswith("\t")
        )[:2000].strip()

    if len(raw_logs) < 50:
        # Fallback 2: strip email headers and use the message body directly.
        # Many valid bug reports are plain-text descriptions without log blocks.
        body_lines = []
        in_headers = True
        for line in first_raw.splitlines():
            if in_headers and line.strip() == "":
                in_headers = False
                continue
            if not in_headers:
                body_lines.append(line)
        raw_logs = "\n".join(body_lines).strip()[:2000]

    if len(raw_logs) < 50:
        return None

    # Debug steps: middle messages (maintainer Q&A)
    debug_msgs  = messages[1:-2] if len(messages) > 3 else []
    debug_steps = "\n---\n".join(m["raw"][:400] for m in debug_msgs[:5]).strip()

    # Root cause: look for diagnosis language
    root_cause  = "unknown"
    for pat in [
        r"(?:the (?:bug|issue|cause) (?:is|was)|root cause)[:\s]+(.{20,400})",
        r"(?:this happens because|triggered by)[:\s]+(.{20,300})",
        r"(?:introduced in|regression since)[:\s]+(.{20,200})",
    ]:
        m2 = re.search(pat, all_text, re.IGNORECASE | re.DOTALL)
        if m2:
            root_cause = m2.group(1).strip()[:400]
            break

    # Solution: last message or message with patch
    solution = ""
    for msg_d in reversed(messages):
        if _PATCH_RE.search(msg_d["raw"]) or _COMMIT_RE.search(msg_d["raw"]):
            solution = msg_d["raw"][:800].strip()
            break
    if not solution:
        solution = messages[-1]["raw"][:800].strip()

    # Reasoning
    reasoning = ""
    for pat in [r"(?:this fix|the fix|solution)[:\s]+(.{20,400})", r"because[:\s]+(.{20,300})"]:
        m2 = re.search(pat, solution, re.IGNORECASE | re.DOTALL)
        if m2:
            reasoning = m2.group(1).strip()[:400]
            break

    # Environment
    distro    = extract_distro(all_text)
    kernel    = extract_kernel(all_text)
    component = extract_component(all_text)

    # Version scope
    versions  = _VERSION_RE.findall(all_text)
    ver_scope = ", ".join(sorted(set(versions))[:6]) if versions else "unknown"

    # Domain from list name
    domain = _DOMAIN_HINTS.get(list_name, "kernel")

    # Failure type
    failure_type = "other"
    combined = (subject + " " + first_raw[:2000]).lower()
    if "panic" in combined:
        failure_type = "kernel panic"
    elif "segfault" in combined or "segmentation fault" in combined:
        failure_type = "segfault"
    elif "use-after-free" in combined or "null pointer" in combined:
        failure_type = "segfault"
    elif "permission" in combined:
        failure_type = "permission"
    elif "timeout" in combined:
        failure_type = "network timeout"
    elif "corrupt" in combined:
        failure_type = "disk corruption"

    # doc_id: hash of thread URL
    import hashlib
    uid = hashlib.md5(thread_url.encode()).hexdigest()[:12]

    doc = LinuxLynxDoc.build(
        doc_id        = f"lkml_{uid}",
        source        = "lkml",
        domain        = domain,
        failure_type  = failure_type,
        distro        = distro,
        kernel        = kernel,
        component     = component,
        problem       = subject or "Unknown kernel issue",
        raw_logs      = raw_logs[:2000],
        debug_steps   = debug_steps[:1000],
        root_cause    = root_cause,
        solution      = solution,
        reasoning     = reasoning,
        version_scope = ver_scope,
        confidence    = "medium",   # mailing list = medium (needs confirmation)
        link          = thread_url,
    )

    errs = doc.validate()
    if errs:
        log.debug("Thread %s validation: %s", thread_url, errs)
        return None

    return doc


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape(
    lists: list[str] = LISTS,
    keywords: list[str] = FAILURE_KEYWORDS,
    max_threads_per_combo: int = 10,
    max_total: int = MAX_TOTAL_DOCS,
) -> Generator[LinuxLynxDoc, None, None]:
    """
    Yield LinuxLynxDoc for resolved kernel mailing-list threads.
    """
    seen_urls: set[str] = set()
    total = 0

    for list_name in lists:
        if total >= max_total:
            break
        domain_hint = _DOMAIN_HINTS.get(list_name, "kernel")
        log.info("Scanning list=%s", list_name)

        for keyword in keywords:
            if total >= max_total:
                break
            log.info("  keyword=%r", keyword)

            thread_urls = _search_list(list_name, keyword, max_threads_per_combo)
            log.info("  found %d candidate threads", len(thread_urls))
            time.sleep(DELAY_SECONDS)

            for t_url in thread_urls:
                if t_url in seen_urls or total >= max_total:
                    continue
                seen_urls.add(t_url)

                log.info("    fetching thread: %s", t_url)
                messages = _fetch_thread_messages(t_url)
                time.sleep(DELAY_SECONDS)

                if not _is_valid_thread(messages):
                    log.debug("    thread invalid/unresolved, skipping")
                    continue

                doc = _build_doc(t_url, messages, list_name)
                if doc:
                    total += 1
                    log.info("    → accepted doc %d: %s", total, doc.doc_id)
                    yield doc


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator
    deduper = Deduplicator()
    
    out = sys.argv[1] if len(sys.argv) > 1 else "lkml.jsonl"
    count = 0
    with open(out, "w") as fh:
        for doc in scrape(max_total=5):
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if not deduper.is_duplicate(content_to_hash):
                fh.write(doc.to_jsonl() + "\n")
                count += 1
                print(f"[{count}] NEW: {doc.doc_id}: {doc.problem[:60]}")
            else:
                print(f"[-] DUP: {doc.doc_id}")
        deduper._save_hashes()
    print(f"\nWrote {count} docs → {out}")
