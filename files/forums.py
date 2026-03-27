"""
Forum scraper — LinuxQuestions.org, Arch BBS, Ubuntu Forums
============================================================
Only scrapes threads marked [SOLVED] or with an accepted solution post.

Per-site strategies
-------------------
LinuxQuestions:
  - Search: https://www.linuxquestions.org/questions/search.php?do=process
  - Filter: subject contains [SOLVED]
  - Parse: first post = problem+logs, solution post identified by "SOLVED" marker

Arch BBS:
  - Search: https://bbs.archlinux.org/search.php
  - Filter: subject tag [SOLVED]
  - Parse: similar structure to linuxquestions

Ubuntu Forums:
  - Search via Google CSE fallback (site:ubuntuforums.org [SOLVED] <keyword>)
  - Parse solved threads

All three use similar phpBB-style HTML — shared parser covers them.
"""

from __future__ import annotations
import re
import time
import logging
import hashlib
from typing import Generator
from urllib.parse import urljoin, urlencode, quote_plus

import requests
from bs4 import BeautifulSoup

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from schema import (
    LinuxLynxDoc, extract_distro, extract_kernel, extract_component
)

log = logging.getLogger(__name__)

DELAY     = 2.0
MAX_PAGES = 10   # search result pages per keyword
MAX_DOCS  = 200

HEADERS = {
    "User-Agent": "LinuxLynx-DataCollector/1.0 (research dataset; contact: dataset@linuxlynx.dev)",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Site configs ──────────────────────────────────────────────────────────────

SITES = {
    "linuxquestions": {
        "base":         "https://www.linuxquestions.org",
        "search_url":   "https://www.linuxquestions.org/questions/search.php",
        "search_params": {
            "do":           "process",
            "titleonly":    "1",
            "query":        "{keyword} [SOLVED]",
            "action":       "showresults",
            "sortby":       "lastpost",
            "order":        "descending",
        },
        "result_link_pattern": re.compile(r"/questions/\S+/\d+"),
        "post_selector":       ".postbody",
        "solved_marker":       re.compile(r"\[SOLVED\]", re.IGNORECASE),
        "source_name":         "forum",
        "confidence":          "medium",
    },
    "arch_bbs": {
        "base":         "https://bbs.archlinux.org",
        "search_url":   "https://bbs.archlinux.org/search.php",
        "search_params": {
            "action":   "search",
            "keywords": "{keyword}",
            "subject":  "1",
            "search_in":"title",
        },
        "result_link_pattern": re.compile(r"/viewtopic\.php\?id=\d+"),
        "post_selector":       ".post-body",
        "solved_marker":       re.compile(r"\[SOLVED\]|\[solved\]"),
        "source_name":         "forum",
        "confidence":          "medium",
    },
    "ubuntu_forums": {
        "base":         "https://ubuntuforums.org",
        "search_url":   "https://ubuntuforums.org/search.php",
        "search_params": {
            "do":       "process",
            "titleonly":"1",
            "query":    "[SOLVED] {keyword}",
        },
        "result_link_pattern": re.compile(r"/showthread\.php\?t=\d+"),
        "post_selector":       ".postbody",
        "solved_marker":       re.compile(r"\[SOLVED\]|\[solved\]", re.IGNORECASE),
        "source_name":         "forum",
        "confidence":          "medium",
    },
}

# Keywords to drive searches across all forums
SEARCH_KEYWORDS = [
    "kernel panic",
    "segmentation fault",
    "network interface down",
    "filesystem read-only",
    "grub rescue",
    "systemd service failed",
    "permission denied ssh",
    "package dependency broken",
    "boot failure initramfs",
    "iptables not working",
    "ext4 corruption",
    "dns resolution fails",
    "module not found",
    "dbus error",
    "selinux denied",
]

# ── Patterns ─────────────────────────────────────────────────────────────────

_CODE_STRIP = re.compile(r"<(?!/?(?:pre|code))[^>]+>")
_WHITESPACE = re.compile(r"\n{3,}")
_FAILURE_TYPE_MAP = [
    (re.compile(r"panic",        re.I), "kernel panic"),
    (re.compile(r"segfault|segmentation fault", re.I), "segfault"),
    (re.compile(r"permission denied", re.I), "permission"),
    (re.compile(r"timeout",      re.I), "network timeout"),
    (re.compile(r"corrupt",      re.I), "disk corruption"),
    (re.compile(r"depend|missi", re.I), "dependency"),
    (re.compile(r"config",       re.I), "config error"),
]
_DOMAIN_MAP = [
    (re.compile(r"network|wifi|eth|dns|route|iptables|nftables|firewall", re.I), "networking"),
    (re.compile(r"kernel|oops|panic|dmesg|kvm|module",  re.I), "kernel"),
    (re.compile(r"systemd|service|unit|journal",        re.I), "systemd"),
    (re.compile(r"ext[234]|btrfs|xfs|zfs|mount|fstab", re.I), "filesystem"),
    (re.compile(r"ssh|selinux|apparmor|sudo|permission",re.I), "security"),
    (re.compile(r"grub|uefi|boot|initramfs",            re.I), "boot"),
    (re.compile(r"apt|dpkg|yum|pacman|pip|depend",      re.I), "package"),
    (re.compile(r"docker|podman|container|lxc",         re.I), "container"),
    (re.compile(r"lvm|raid|mdadm|disk|nvme|sata",       re.I), "storage"),
    (re.compile(r"memory|oom|swap|malloc",              re.I), "memory"),
    (re.compile(r"qemu|virtualbox|vmware|virt",         re.I), "virtualization"),
]


def _infer_domain(text: str) -> str:
    for pat, domain in _DOMAIN_MAP:
        if pat.search(text):
            return domain
    return "other"


def _infer_failure_type(text: str) -> str:
    for pat, ft in _FAILURE_TYPE_MAP:
        if pat.search(text):
            return ft
    return "other"


def _clean_post(html: str) -> str:
    """Extract text from a forum post, preserving code blocks."""
    soup = BeautifulSoup(html, "html.parser")
    # Pull code blocks first
    code_parts = []
    for tag in soup.find_all(["pre", "code"]):
        t = tag.get_text()
        if len(t.strip()) > 10:
            code_parts.append(t.strip())
        tag.replace_with("\n[CODE]\n" + t + "\n[/CODE]\n")
    text = soup.get_text(separator="\n")
    return _WHITESPACE.sub("\n\n", text).strip()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(url: str, params: dict | None = None, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning("GET %s failed (attempt %d): %s", url, attempt + 1, e)
            time.sleep(DELAY * (attempt + 1))
    return None


# ── Thread parser ─────────────────────────────────────────────────────────────

def _parse_thread(thread_url: str, site_cfg: dict) -> LinuxLynxDoc | None:
    """Parse a single forum thread and return a LinuxLynxDoc or None."""
    r = _get(thread_url)
    if not r:
        return None

    soup     = BeautifulSoup(r.text, "html.parser")
    base_url = site_cfg["base"]

    # Thread title
    title_tag = (
        soup.find("h1") or
        soup.find("title") or
        soup.find(class_=re.compile(r"thread-?title|post-?title", re.I))
    )
    title = title_tag.get_text(strip=True) if title_tag else "Unknown issue"
    title = re.sub(r"\[SOLVED\]\s*", "", title, flags=re.I).strip()

    # Gather all posts
    posts = soup.find_all(class_=re.compile(
        r"postbody|post-body|post-content|message-body|postcontent", re.I
    ))
    if not posts:
        # fallback: any <div> with post-ish class
        posts = soup.find_all("div", class_=re.compile(r"post", re.I))

    if len(posts) < 2:
        log.debug("Thread %s: too few posts (%d)", thread_url, len(posts))
        return None

    # First post = problem description
    first_post = _clean_post(str(posts[0]))

    # Extract raw_logs from first post (code blocks)
    raw_logs = ""
    for tag in BeautifulSoup(str(posts[0]), "html.parser").find_all(["pre", "code"]):
        raw_logs += tag.get_text() + "\n"
    raw_logs = raw_logs[:2000].strip()

    if len(raw_logs) < 50:
        # Try indented lines as fallback
        raw_logs = "\n".join(
            ln for ln in first_post.splitlines()
            if ln.startswith("  ") or ln.startswith("\t") or
               re.match(r"^\[[\s\d.]+\]|Error:|Warning:|Failed|FAILED", ln)
        )[:2000].strip()

    if len(raw_logs) < 50:
        log.debug("Thread %s: raw_logs too short", thread_url)
        return None

    # Identify solution post — last post, or post containing fix/solved language
    solution_post  = _clean_post(str(posts[-1]))
    solution_text  = solution_post

    for post_html in reversed(list(posts[1:])):
        post_text = _clean_post(str(post_html))
        if re.search(r"fix|solved|resolv|answer|solution|work(?:ed|s)", post_text, re.I):
            solution_text = post_text
            break

    # Debug steps: middle posts
    middle_posts  = posts[1:-1]
    debug_steps   = "\n---\n".join(
        _clean_post(str(p))[:400] for p in middle_posts[:5]
    ).strip()

    all_text      = first_post + "\n" + solution_text

    # Environment
    distro    = extract_distro(all_text)
    kernel    = extract_kernel(all_text)
    component = extract_component(all_text)

    # Root cause inference
    root_cause = "unknown"
    for pat in [
        r"(?:root cause|the (?:problem|issue|bug) (?:was|is))[:\s]+(.{20,300})",
        r"(?:caused by|this happens because)[:\s]+(.{20,300})",
    ]:
        m = re.search(pat, all_text, re.IGNORECASE | re.DOTALL)
        if m:
            root_cause = m.group(1).strip()[:400]
            break

    # Reasoning
    reasoning = ""
    for pat in [
        r"(?:this fix|the fix|solution)[:\s]+(.{20,300})",
        r"because[:\s]+(.{20,250})",
    ]:
        m = re.search(pat, solution_text, re.IGNORECASE | re.DOTALL)
        if m:
            reasoning = m.group(1).strip()[:400]
            break

    uid = hashlib.md5(thread_url.encode()).hexdigest()[:12]

    doc = LinuxLynxDoc.build(
        doc_id        = f"forum_{uid}",
        source        = site_cfg["source_name"],
        domain        = _infer_domain(all_text),
        failure_type  = _infer_failure_type(all_text),
        distro        = distro,
        kernel        = kernel,
        component     = component,
        problem       = title[:200],
        raw_logs      = raw_logs,
        debug_steps   = debug_steps[:1000],
        root_cause    = root_cause,
        solution      = solution_text[:800],
        reasoning     = reasoning,
        version_scope = "unknown",
        confidence    = site_cfg["confidence"],
        link          = thread_url,
    )

    errs = doc.validate()
    if errs:
        log.debug("Thread %s validation: %s", thread_url, errs)
        return None

    return doc


# ── Per-site search ───────────────────────────────────────────────────────────

def _search_site(site_name: str, cfg: dict, keyword: str, max_pages: int) -> list[str]:
    """Return thread URLs matching keyword from a specific forum site."""
    urls: list[str] = []
    base = cfg["base"]
    link_pat = cfg["result_link_pattern"]
    solved_re = cfg["solved_marker"]

    for page in range(max_pages):
        params = {
            k: v.replace("{keyword}", keyword) if isinstance(v, str) else v
            for k, v in cfg["search_params"].items()
        }
        if page > 0:
            params["page"] = str(page + 1)
            params["start"] = str(page * 25)

        r = _get(cfg["search_url"], params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        found_any = False

        for a in soup.find_all("a", href=True):
            href = a["href"]
            link_text = a.get_text(strip=True)

            if not link_pat.search(href):
                continue
            if not solved_re.search(link_text + href):
                # Check parent element text
                parent_text = a.parent.get_text() if a.parent else ""
                if not solved_re.search(parent_text):
                    continue

            full = urljoin(base, href)
            if full not in urls:
                urls.append(full)
                found_any = True

        if not found_any:
            break

        time.sleep(DELAY)

    return urls


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape(
    site_names: list[str] | None = None,
    keywords: list[str] = SEARCH_KEYWORDS,
    max_docs: int = MAX_DOCS,
) -> Generator[LinuxLynxDoc, None, None]:
    """
    Yield LinuxLynxDoc from [SOLVED] forum threads across configured sites.
    """
    active_sites = {
        k: v for k, v in SITES.items()
        if site_names is None or k in site_names
    }

    seen_urls: set[str] = set()
    total = 0

    for site_name, cfg in active_sites.items():
        if total >= max_docs:
            break
        log.info("Searching site: %s", site_name)

        for keyword in keywords:
            if total >= max_docs:
                break
            log.info("  keyword=%r", keyword)

            thread_urls = _search_site(site_name, cfg, keyword, max_pages=MAX_PAGES)
            log.info("  found %d candidate threads", len(thread_urls))

            for t_url in thread_urls:
                if t_url in seen_urls or total >= max_docs:
                    continue
                seen_urls.add(t_url)

                log.info("    parsing: %s", t_url)
                time.sleep(DELAY)

                doc = _parse_thread(t_url, cfg)
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
    
    out = sys.argv[1] if len(sys.argv) > 1 else "forums.jsonl"
    count = 0
    with open(out, "w") as fh:
        for doc in scrape(max_docs=5):
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if not deduper.is_duplicate(content_to_hash):
                fh.write(doc.to_jsonl() + "\n")
                count += 1
                print(f"[{count}] NEW: {doc.doc_id}: {doc.problem[:70]}")
            else:
                print(f"[-] DUP: {doc.doc_id}")
        deduper._save_hashes()
    print(f"\nWrote {count} docs → {out}")
