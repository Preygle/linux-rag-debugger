"""
LKML scraper
============

The original lore.kernel.org flow now lands on an Anubis proof-of-work page,
which yields zero threads to plain HTTP clients. This version uses MARC's
linux-kernel archive instead, where search results, thread pages, and message
pages remain fetchable without JavaScript.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

import sys
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.schema import LinuxLynxDoc, extract_component, extract_distro, extract_kernel

log = logging.getLogger(__name__)

MARC_BASE = "https://marc.info/"
LISTS = ["linux-kernel"]

FAILURE_KEYWORDS = [
    "kernel panic",
    "BUG:",
    "hard lockup",
    "use-after-free",
    "null pointer",
    "WARNING:",
    "oops",
    "call trace",
    "page fault",
    "hung task",
    "deadlock",
    "memory leak",
]

DELAY_SECONDS = 0.2
MAX_THREADS_PER_KEYWORD = 6
MAX_TOTAL_DOCS = 300
MAX_WORKERS = 4
MAX_SEARCH_PAGES = 1

HEADERS = {
    "User-Agent": "LinuxLynx-DataCollector/2.0 (research; contact: dataset@linuxlynx.dev)"
}

_PATCH_RE = re.compile(r"^diff --git|^\+{3}\s|^-{3}\s|^index [0-9a-f]+\.\.", re.M)
_COMMIT_RE = re.compile(r"\b[0-9a-f]{12,40}\b")
_FIXED_RE = re.compile(r"(?:fix(?:ed)?|applied|merged|resolved|works now|confirmed)", re.I)
_RFC_RE = re.compile(r"\[RFC\]|\[PATCH.*WIP\]|\[PATCH v\d+ WIP\]", re.I)
_VERSION_RE = re.compile(r"\b(\d+\.\d+(?:\.\d+)?(?:-[\w.]+)?)\b")
_SUBJECT_JUNK = re.compile(r"^\s*(?:Re:\s*)+|\[PATCH[^\]]*\]\s*|\[RFC[^\]]*\]\s*", re.I)
_SUBJECT_FAILURE_RE = re.compile(
    r"\[bug\]|regression|kernel panic|hard lockup|use-after-free|null pointer|"
    r"warning|oops|call trace|page fault|hung task|deadlock|memory leak",
    re.I,
)
_FAILURE_RE = re.compile(
    r"kernel panic|bug:|hard lockup|use-after-free|null pointer|warning:|"
    r"oops|call trace|page fault|hung task|deadlock|memory leak",
    re.I,
)
_LOG_LINE_RE = re.compile(
    r"^\[[\s\d.]+\]|^BUG:|^WARNING:|^Oops|^Call Trace|^RIP:|^Kernel panic|"
    r"^watchdog:|^CPU:\s+\d+",
    re.I,
)


def _marc_url(params: dict[str, str | int]) -> str:
    return f"{MARC_BASE}?{urlencode(params)}"


def _get(url: str, params: dict | None = None, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=25)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt + 1, retries, exc)
            time.sleep(DELAY_SECONDS * (attempt + 1))
    return None


def _extract_message_body(pre_text: str) -> str:
    parts = pre_text.split("\n\n", 1)
    return parts[1].strip() if len(parts) == 2 else pre_text.strip()


def _fetch_message(message_url: str) -> dict | None:
    response = _get(message_url)
    if not response:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    pre = soup.find("pre")
    raw = pre.get_text("\n") if pre else soup.get_text("\n")

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    subject = re.sub(r"\s*-\s*MARC$", "", title).strip("' ")
    if not subject:
        match = re.search(r"^Subject:\s*(.+)$", raw, re.MULTILINE)
        subject = match.group(1).strip() if match else ""

    thread_url = ""
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "t=" in href and "w=2" in href:
            thread_url = urljoin(MARC_BASE, href)
            break

    return {
        "url": message_url,
        "thread_url": thread_url,
        "subject": subject,
        "raw": raw.strip(),
        "body": _extract_message_body(raw),
    }


def _search_threads(keyword: str, max_threads: int) -> list[str]:
    thread_urls: list[str] = []
    seen_threads: set[str] = set()

    for page in range(1, MAX_SEARCH_PAGES + 1):
        if len(thread_urls) >= max_threads:
            break

        response = _get(
            MARC_BASE,
            params={"l": "linux-kernel", "q": "b", "s": keyword, "r": page, "w": 2},
        )
        if not response:
            break

        soup = BeautifulSoup(response.text, "html.parser")
        message_urls: list[str] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if "l=linux-kernel" in href and "m=" in href:
                full_url = urljoin(MARC_BASE, href)
                if full_url not in message_urls:
                    message_urls.append(full_url)

        if not message_urls:
            break

        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(message_urls))) as executor:
            futures = {
                executor.submit(_fetch_message, message_url): message_url
                for message_url in message_urls[: max_threads * 2]
            }
            for future in as_completed(futures):
                message = future.result()
                if not message or not message["thread_url"]:
                    continue
                if message["thread_url"] in seen_threads:
                    continue
                seen_threads.add(message["thread_url"])
                thread_urls.append(message["thread_url"])
                if len(thread_urls) >= max_threads:
                    break

        time.sleep(DELAY_SECONDS)

    return thread_urls


def _fetch_thread_messages(thread_url: str) -> list[dict]:
    response = _get(thread_url)
    if not response:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    container = soup.find("pre") or soup
    message_urls: list[str] = []
    for anchor in container.find_all("a", href=True):
        href = anchor["href"]
        if "l=linux-kernel" in href and "m=" in href:
            full_url = urljoin(MARC_BASE, href)
            if full_url not in message_urls:
                message_urls.append(full_url)

    if not message_urls:
        return []

    original_index = len(message_urls) - 1
    resolution_index = 0
    indexed_urls = [(original_index, message_urls[original_index])]
    if len(message_urls) > 1:
        indexed_urls.append((resolution_index, message_urls[resolution_index]))
    fetched: list[tuple[int, dict]] = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(indexed_urls))) as executor:
        futures = {
            executor.submit(_fetch_message, message_url): index
            for index, message_url in indexed_urls
        }
        for future in as_completed(futures):
            message = future.result()
            if message:
                fetched.append((futures[future], message))

    fetched.sort(key=lambda item: item[0], reverse=True)
    return [message for _, message in fetched]


def _infer_domain(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("netdev", "ethernet", "wifi", "tcp", "udp", "nftables")):
        return "networking"
    if any(term in lowered for term in ("ext4", "btrfs", "xfs", "fsdevel", "mount")):
        return "filesystem"
    if any(term in lowered for term in ("selinux", "apparmor", "permission", "security")):
        return "security"
    if any(term in lowered for term in ("grub", "boot", "initramfs", "dracut")):
        return "boot"
    if any(term in lowered for term in ("memory leak", "mm/", "page fault")):
        return "memory"
    return "kernel"


def _infer_failure_type(text: str) -> str:
    lowered = text.lower()
    if "panic" in lowered:
        return "kernel panic"
    if "segfault" in lowered or "use-after-free" in lowered or "null pointer" in lowered:
        return "segfault"
    if "permission" in lowered:
        return "permission"
    if "timeout" in lowered or "lockup" in lowered:
        return "network timeout"
    if "corrupt" in lowered:
        return "disk corruption"
    return "other"


def _pick_raw_logs(body: str) -> str:
    log_lines = [line for line in body.splitlines() if _LOG_LINE_RE.search(line)]
    raw_logs = "\n".join(log_lines[:80]).strip()
    if len(raw_logs) >= 50:
        return raw_logs[:2000]

    indented = "\n".join(
        line for line in body.splitlines()
        if line.startswith("  ") or line.startswith("\t")
    ).strip()
    if len(indented) >= 50:
        return indented[:2000]

    return body[:2000].strip()


def _is_valid_thread(messages: list[dict]) -> bool:
    if len(messages) < 2:
        return False

    subject = messages[0]["subject"]
    if _RFC_RE.search(subject):
        return False
    if not _SUBJECT_FAILURE_RE.search(subject):
        return False

    opening_text = f"{subject}\n{messages[0]['body']}"
    if not _FAILURE_RE.search(opening_text):
        return False

    replies_text = "\n".join(message["body"] for message in messages[1:])
    if not (_PATCH_RE.search(replies_text) or _COMMIT_RE.search(replies_text) or _FIXED_RE.search(replies_text)):
        return False

    return True


def _build_doc(thread_url: str, messages: list[dict]) -> LinuxLynxDoc | None:
    if not messages:
        return None

    subject = _SUBJECT_JUNK.sub("", messages[0]["subject"]).strip()
    first_body = messages[0]["body"]
    raw_logs = _pick_raw_logs(first_body)
    if len(raw_logs) < 50:
        return None

    debug_steps = "\n---\n".join(message["body"][:350] for message in messages[1:-1]).strip()

    solution = ""
    for message in reversed(messages[1:]):
        body = message["body"]
        if _PATCH_RE.search(body) or _COMMIT_RE.search(body) or _FIXED_RE.search(body):
            solution = body[:900].strip()
            break
    if not solution:
        solution = messages[-1]["body"][:900].strip()

    all_text = "\n\n".join(message["body"] for message in messages)
    root_cause = "unknown"
    for pattern in [
        r"(?:root cause|the (?:bug|issue|problem) (?:is|was))[:\s]+(.{20,350})",
        r"(?:triggered by|caused by|because)[:\s]+(.{20,320})",
        r"(?:introduced in|regression since)[:\s]+(.{20,250})",
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
        match = re.search(pattern, solution, re.IGNORECASE | re.DOTALL)
        if match:
            reasoning = match.group(1).strip()[:400]
            break

    version_scope = ", ".join(dict.fromkeys(_VERSION_RE.findall(all_text))) or "unknown"
    doc = LinuxLynxDoc.build(
        doc_id=f"lkml_{hashlib.md5(thread_url.encode()).hexdigest()[:12]}",
        source="lkml",
        domain=_infer_domain(f"{subject}\n{all_text}"),
        failure_type=_infer_failure_type(f"{subject}\n{all_text}"),
        distro=extract_distro(all_text),
        kernel=extract_kernel(all_text),
        component=extract_component(f"{subject}\n{all_text}"),
        problem=subject or "Unknown kernel issue",
        raw_logs=raw_logs,
        debug_steps=debug_steps[:1000],
        root_cause=root_cause,
        solution=solution,
        reasoning=reasoning,
        version_scope=version_scope[:200],
        confidence="medium",
        link=thread_url,
    )

    errors = doc.validate()
    if errors:
        log.debug("Thread %s validation errors: %s", thread_url, errors)
        return None
    return doc


def _build_thread_doc(thread_url: str) -> LinuxLynxDoc | None:
    messages = _fetch_thread_messages(thread_url)
    if not _is_valid_thread(messages):
        return None
    return _build_doc(thread_url, messages)


def scrape(
    lists: list[str] = LISTS,
    keywords: list[str] = FAILURE_KEYWORDS,
    max_threads_per_combo: int = MAX_THREADS_PER_KEYWORD,
    max_total: int = MAX_TOTAL_DOCS,
) -> Generator[LinuxLynxDoc, None, None]:
    """
    Yield LinuxLynxDoc objects from recent linux-kernel threads.
    """
    del lists  # MARC search is scoped to linux-kernel only.

    seen_threads: set[str] = set()
    total = 0

    for keyword in keywords:
        if total >= max_total:
            break

        thread_urls = _search_threads(keyword, max_threads_per_combo)
        candidates = [thread_url for thread_url in thread_urls if thread_url not in seen_threads]
        seen_threads.update(candidates)
        if not candidates:
            continue

        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates))) as executor:
            futures = {
                executor.submit(_build_thread_doc, thread_url): thread_url
                for thread_url in candidates
            }
            for future in as_completed(futures):
                doc = future.result()
                if not doc:
                    continue
                total += 1
                yield doc
                if total >= max_total:
                    return


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator

    deduper = Deduplicator()
    output = sys.argv[1] if len(sys.argv) > 1 else "lkml.jsonl"
    count = 0

    with open(output, "w", encoding="utf-8") as handle:
        for doc in scrape(max_total=5):
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if deduper.is_duplicate(content_to_hash):
                print(f"[-] DUP: {doc.doc_id}")
                continue
            handle.write(doc.to_jsonl() + "\n")
            count += 1
            print(f"[{count}] NEW: {doc.doc_id}: {doc.problem[:70]}")

    deduper._save_hashes()
    print(f"\nWrote {count} docs -> {output}")
