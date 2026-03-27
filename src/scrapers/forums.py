"""
Forum scraper
=============

Arch Linux BBS is the primary default source because it is reachable from the
pipeline and exposes solved/resolved threads without anti-bot interstitials.
LinuxQuestions and Ubuntu Forums remain supported when explicitly requested,
but they are disabled by default because they currently return Cloudflare or
connection-reset responses in this environment.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator
from urllib.parse import urljoin

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

DELAY = 0.35
MAX_PAGES = 4
MAX_DOCS = 200
MAX_WORKERS = 8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_BLOCK_MARKERS = (
    "just a moment",
    "making sure you're not a bot",
    "making sure you&#39;re not a bot",
    "enable javascript",
    "attention required",
)

SITES = {
    "linuxquestions": {
        "base": "https://www.linuxquestions.org",
        "search_url": "https://www.linuxquestions.org/questions/search.php",
        "search_params": {
            "do": "process",
            "titleonly": "1",
            "query": "{keyword} [SOLVED]",
            "action": "showresults",
            "sortby": "lastpost",
            "order": "descending",
        },
        "result_link_pattern": re.compile(r"/questions/\S+/\d+"),
        "post_selector": ".postbody",
        "solved_marker": re.compile(r"\[(?:solved|resolved|fixed)\]|\b(?:solved|resolved)\b", re.I),
        "source_name": "forum",
        "confidence": "medium",
        "default_enabled": False,
    },
    "arch_bbs": {
        "base": "https://bbs.archlinux.org",
        "search_url": "https://bbs.archlinux.org/search.php",
        "search_params": {
            "action": "search",
            "keywords": "{keyword}",
            "subject": "1",
            "search_in": "title",
        },
        "result_link_pattern": re.compile(r"/?viewtopic\.php\?id=\d+"),
        "post_selector": ".post-body",
        "solved_marker": re.compile(r"\[(?:solved|resolved|fixed)\]|\b(?:solved|resolved)\b", re.I),
        "source_name": "forum",
        "confidence": "medium",
        "default_enabled": True,
    },
    "ubuntu_forums": {
        "base": "https://ubuntuforums.org",
        "search_url": "https://ubuntuforums.org/search.php",
        "search_params": {
            "do": "process",
            "titleonly": "1",
            "query": "[SOLVED] {keyword}",
        },
        "result_link_pattern": re.compile(r"/showthread\.php\?t=\d+"),
        "post_selector": ".postbody",
        "solved_marker": re.compile(r"\[(?:solved|resolved|fixed)\]|\b(?:solved|resolved)\b", re.I),
        "source_name": "forum",
        "confidence": "medium",
        "default_enabled": False,
    },
}

SEARCH_KEYWORDS = [
    "systemd service failed",
    "dns resolution fails",
    "boot failure initramfs",
    "permission denied ssh",
    "kernel panic",
    "segmentation fault",
    "network interface down",
    "filesystem read-only",
    "grub rescue",
    "package dependency broken",
    "iptables not working",
    "ext4 corruption",
    "module not found",
    "dbus error",
    "selinux denied",
]

_WHITESPACE = re.compile(r"\n{3,}")
_SOLVED_TAG_RE = re.compile(r"\[(?:solved|resolved|fixed)\]\s*", re.I)
_FAILURE_TYPE_MAP = [
    (re.compile(r"panic", re.I), "kernel panic"),
    (re.compile(r"segfault|segmentation fault", re.I), "segfault"),
    (re.compile(r"permission denied|selinux|apparmor", re.I), "permission"),
    (re.compile(r"timeout", re.I), "network timeout"),
    (re.compile(r"corrupt", re.I), "disk corruption"),
    (re.compile(r"depend|missing module|package conflict", re.I), "dependency"),
    (re.compile(r"config|misconfig", re.I), "config error"),
]
_DOMAIN_MAP = [
    (re.compile(r"network|wifi|eth|dns|route|iptables|nftables|firewall", re.I), "networking"),
    (re.compile(r"kernel|oops|panic|dmesg|module|kdump", re.I), "kernel"),
    (re.compile(r"systemd|service|unit|journal|dbus", re.I), "systemd"),
    (re.compile(r"ext[234]|btrfs|xfs|zfs|mount|fstab", re.I), "filesystem"),
    (re.compile(r"ssh|selinux|apparmor|sudo|permission", re.I), "security"),
    (re.compile(r"grub|uefi|boot|initramfs|dracut", re.I), "boot"),
    (re.compile(r"apt|dpkg|yum|pacman|zypper|depend", re.I), "package"),
    (re.compile(r"docker|podman|container|lxc", re.I), "container"),
    (re.compile(r"lvm|raid|mdadm|disk|nvme|sata", re.I), "storage"),
    (re.compile(r"memory|oom|swap|malloc", re.I), "memory"),
    (re.compile(r"qemu|virtualbox|vmware|virt", re.I), "virtualization"),
]


def _response_is_blocked(response: requests.Response) -> bool:
    lowered = response.text.lower()
    return response.status_code in {403, 429} or any(marker in lowered for marker in _BLOCK_MARKERS)


def _infer_domain(text: str) -> str:
    for pattern, domain in _DOMAIN_MAP:
        if pattern.search(text):
            return domain
    return "other"


def _infer_failure_type(text: str) -> str:
    for pattern, failure_type in _FAILURE_TYPE_MAP:
        if pattern.search(text):
            return failure_type
    return "other"


def _clean_post(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["pre", "code"]):
        code_text = tag.get_text()
        tag.replace_with("\n[CODE]\n" + code_text + "\n[/CODE]\n")
    return _WHITESPACE.sub("\n\n", soup.get_text(separator="\n")).strip()


def _get(url: str, params: dict | None = None, retries: int = 2) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=25)
            if _response_is_blocked(response):
                log.info("Skipping blocked forum page %s", response.url)
                return None
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            message = str(exc).lower()
            if "connection aborted" in message or "forcibly closed" in message:
                log.info("Skipping unreachable forum page %s: %s", url, exc)
                return None
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt + 1, retries, exc)
            time.sleep(DELAY * (attempt + 1))
    return None


def _extract_thread_title(soup: BeautifulSoup) -> str:
    candidates: list[str] = []

    if soup.title:
        candidates.append(soup.title.get_text(" ", strip=True))

    crumbs = soup.find(class_=re.compile(r"crumbs|breadcrumb", re.I))
    if crumbs:
        crumb_links = crumbs.find_all("a")
        if crumb_links:
            candidates.append(crumb_links[-1].get_text(" ", strip=True))
        else:
            candidates.append(crumbs.get_text(" ", strip=True))

    custom_heading = soup.find(class_=re.compile(r"thread-?title|topic-?title|post-?title", re.I))
    if custom_heading:
        candidates.append(custom_heading.get_text(" ", strip=True))

    for candidate in candidates:
        title = candidate
        if " / " in title:
            title = title.split(" / ", 1)[0]
        title = re.sub(r"\s*/\s*Arch Linux Forums.*$", "", title, flags=re.I)
        title = re.sub(r"\s*[-|]\s*(?:LinuxQuestions\.org|Ubuntu Forums).*$", "", title, flags=re.I)
        title = re.sub(r"^\s*Index\s+[›»·|]+\s*", "", title, flags=re.I)
        title = _SOLVED_TAG_RE.sub("", title).strip(" -|/·›»")
        if title and title.lower() not in {"arch linux", "linuxquestions.org", "ubuntu forums"}:
            return title[:200]
    return "Unknown issue"


def _parse_thread(thread_url: str, site_cfg: dict) -> LinuxLynxDoc | None:
    response = _get(thread_url)
    if not response:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    title = _extract_thread_title(soup)

    posts = soup.select(site_cfg["post_selector"])
    if not posts:
        posts = soup.find_all(class_=re.compile(r"postbody|post-body|post-content|message-body|postcontent", re.I))
    if not posts:
        posts = soup.find_all("div", class_=re.compile(r"post", re.I))
    if len(posts) < 2:
        return None

    first_post_html = str(posts[0])
    first_post = _clean_post(first_post_html)
    raw_logs = ""
    for tag in BeautifulSoup(first_post_html, "html.parser").find_all(["pre", "code"]):
        text = tag.get_text()
        if len(text.strip()) > 20:
            raw_logs += text.strip() + "\n"
    raw_logs = raw_logs[:2000].strip()

    if len(raw_logs) < 50:
        raw_logs = "\n".join(
            line for line in first_post.splitlines()
            if line.startswith("  ") or line.startswith("\t") or re.search(r"\b(error|warning|failed|trace|panic)\b", line, re.I)
        )[:2000].strip()

    if len(raw_logs) < 50:
        raw_logs = first_post[:2000].strip()

    if len(raw_logs) < 50:
        return None

    solution_text = _clean_post(str(posts[-1]))
    for post_html in reversed(list(posts[1:])):
        post_text = _clean_post(str(post_html))
        if re.search(r"fix|solved|resolved|works now|answer|solution", post_text, re.I):
            solution_text = post_text
            break

    debug_steps = "\n---\n".join(
        _clean_post(str(post))[:350]
        for post in posts[1:-1][:4]
    ).strip()

    all_text = f"{title}\n{first_post}\n{solution_text}"
    distro = extract_distro(all_text)
    kernel = extract_kernel(all_text)
    component = extract_component(all_text)

    root_cause = "unknown"
    for pattern in [
        r"(?:root cause|the (?:problem|issue|bug) (?:was|is))[:\s]+(.{20,320})",
        r"(?:caused by|this happens because)[:\s]+(.{20,320})",
    ]:
        match = re.search(pattern, all_text, re.IGNORECASE | re.DOTALL)
        if match:
            root_cause = match.group(1).strip()[:400]
            break

    reasoning = ""
    for pattern in [
        r"(?:this fix|the fix|solution)[:\s]+(.{20,280})",
        r"because[:\s]+(.{20,240})",
    ]:
        match = re.search(pattern, solution_text, re.IGNORECASE | re.DOTALL)
        if match:
            reasoning = match.group(1).strip()[:400]
            break

    uid = hashlib.md5(thread_url.encode()).hexdigest()[:12]
    doc = LinuxLynxDoc.build(
        doc_id=f"forum_{uid}",
        source=site_cfg["source_name"],
        domain=_infer_domain(all_text),
        failure_type=_infer_failure_type(all_text),
        distro=distro,
        kernel=kernel,
        component=component,
        problem=title,
        raw_logs=raw_logs,
        debug_steps=debug_steps[:1000],
        root_cause=root_cause,
        solution=solution_text[:900],
        reasoning=reasoning,
        version_scope="unknown",
        confidence=site_cfg["confidence"],
        link=thread_url,
    )

    errors = doc.validate()
    if errors:
        log.debug("Thread %s validation errors: %s", thread_url, errors)
        return None
    return doc


def _search_site(site_name: str, cfg: dict, keyword: str, max_pages: int) -> list[str]:
    urls: list[str] = []
    base = cfg["base"]
    link_pattern = cfg["result_link_pattern"]
    solved_re = cfg["solved_marker"]

    for page in range(max_pages):
        params = {
            key: value.replace("{keyword}", keyword) if isinstance(value, str) else value
            for key, value in cfg["search_params"].items()
        }
        if page > 0:
            params["page"] = str(page + 1)
            params["start"] = str(page * 25)

        response = _get(cfg["search_url"], params=params)
        if not response:
            break

        soup = BeautifulSoup(response.text, "html.parser")
        found_any = False

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if not link_pattern.search(href):
                continue

            container = anchor.find_parent(["tr", "li", "div", "dt", "dd", "article"]) or anchor.parent
            link_text = anchor.get_text(" ", strip=True)
            context_text = container.get_text(" ", strip=True) if container else ""

            if not solved_re.search(f"{link_text} {context_text}"):
                continue

            full_url = urljoin(base, href)
            if full_url not in urls:
                urls.append(full_url)
                found_any = True

        if not found_any:
            break
        time.sleep(DELAY)

    return urls


def scrape(
    site_names: list[str] | None = None,
    keywords: list[str] = SEARCH_KEYWORDS,
    max_docs: int = MAX_DOCS,
) -> Generator[LinuxLynxDoc, None, None]:
    """
    Yield LinuxLynxDoc objects from reachable solved forum threads.
    """
    if site_names is None:
        active_sites = {name: cfg for name, cfg in SITES.items() if cfg.get("default_enabled", True)}
    else:
        active_sites = {name: cfg for name, cfg in SITES.items() if name in site_names}

    seen_urls: set[str] = set()
    total = 0

    for site_name, cfg in active_sites.items():
        if total >= max_docs:
            break
        log.info("Searching site: %s", site_name)

        for keyword in keywords:
            if total >= max_docs:
                break

            thread_urls = _search_site(site_name, cfg, keyword, max_pages=MAX_PAGES)
            candidates = [url for url in thread_urls if url not in seen_urls]
            seen_urls.update(candidates)
            if not candidates:
                continue

            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates))) as executor:
                futures = {
                    executor.submit(_parse_thread, url, cfg): url
                    for url in candidates
                }
                for future in as_completed(futures):
                    doc = future.result()
                    if not doc:
                        continue
                    total += 1
                    yield doc
                    if total >= max_docs:
                        return


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator

    deduper = Deduplicator()
    output = sys.argv[1] if len(sys.argv) > 1 else "forums.jsonl"
    count = 0

    with open(output, "w", encoding="utf-8") as handle:
        for doc in scrape(max_docs=5):
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if deduper.is_duplicate(content_to_hash):
                print(f"[-] DUP: {doc.doc_id}")
                continue
            handle.write(doc.to_jsonl() + "\n")
            count += 1
            print(f"[{count}] NEW: {doc.doc_id}: {doc.problem[:70]}")

    deduper._save_hashes()
    print(f"\nWrote {count} docs -> {output}")
