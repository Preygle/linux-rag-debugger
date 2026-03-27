"""
Security data scrapers
======================
1. NVD (National Vulnerability Database) — https://services.nvd.nist.gov/rest/json/cves/2.0
   Maps CVE records to LinuxLynxDoc with domain=security.
   Only includes Linux-affecting CVEs with CVSS score >= 6.0 (medium/high/critical).

2. Syzkaller — https://syzkaller.appspot.com/
   Kernel crash reproducers from continuous fuzzing.
   Includes raw crash logs, repro programs, and status (open/fixed).
   Only fetches bugs with status=fixed.

Both sources produce domain=security (NVD) or domain=kernel (Syzkaller).
"""

from __future__ import annotations
import re
import time
import json
import logging
import hashlib
from typing import Generator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from schema import (
    LinuxLynxDoc, extract_distro, extract_kernel, extract_component
)

log = logging.getLogger(__name__)

DELAY = 1.5

HEADERS = {
    "User-Agent": "LinuxLynx-DataCollector/1.0 (research; contact: dataset@linuxlynx.dev)"
}


# ══════════════════════════════════════════════════════════════════════════════
# Part 1: NVD CVE scraper
# ══════════════════════════════════════════════════════════════════════════════

NVD_API    = "https://services.nvd.nist.gov/rest/json/cves/2.0"
NVD_DELAY  = 6.0   # NVD rate limit: 5 req/30s without API key; 50/30s with key
MAX_CVE    = 200

# Only Linux kernel / system tool CVEs
NVD_KEYWORD_FILTERS = [
    "linux kernel", "systemd", "glibc", "openssh", "sudo",
    "grub", "nfs", "ext4", "btrfs", "iptables", "nftables",
    "docker", "podman", "qemu", "kvm",
]

_CVSS_MIN  = 6.0   # skip informational/low severity

_CWE_TO_FAILURE = {
    "CWE-119": "segfault",   # Buffer overflow
    "CWE-120": "segfault",   # Classic buffer overflow
    "CWE-125": "segfault",   # Out-of-bounds read
    "CWE-787": "segfault",   # Out-of-bounds write
    "CWE-416": "segfault",   # Use-after-free
    "CWE-476": "segfault",   # NULL pointer dereference
    "CWE-269": "permission", # Improper privilege management
    "CWE-284": "permission", # Improper access control
    "CWE-400": "other",      # Resource exhaustion
    "CWE-362": "other",      # Race condition
    "CWE-20":  "other",      # Improper input validation
}


def _nvd_get(params: dict, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(NVD_API, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 403:
                log.warning("NVD rate limit hit, waiting 30s...")
                time.sleep(30)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning("NVD GET failed (attempt %d): %s", attempt + 1, e)
            time.sleep(NVD_DELAY * (attempt + 1))
    return None


def _extract_nvd_component(description: str, cpe_list: list[str]) -> str:
    # Try CPE strings first — they encode component precisely
    for cpe in cpe_list[:3]:
        parts = cpe.split(":")
        if len(parts) >= 5:
            vendor  = parts[3]
            product = parts[4]
            if product not in ("*", "-", ""):
                return product.replace("_", " ")
    return extract_component(description)


def _extract_nvd_version_scope(cpe_list: list[str]) -> str:
    versions = []
    for cpe in cpe_list:
        parts = cpe.split(":")
        if len(parts) >= 6 and parts[5] not in ("*", "-", ""):
            versions.append(parts[5])
    return ", ".join(sorted(set(versions))[:8]) if versions else "unknown"


def _build_nvd_doc(item: dict) -> LinuxLynxDoc | None:
    cve_id  = item.get("id", "")
    metrics = item.get("metrics", {})
    descs   = item.get("descriptions", [])
    vulns   = item.get("configurations", {})
    refs    = item.get("references", [])
    weaknesses = item.get("weaknesses", [])

    # Get English description
    description = ""
    for d in descs:
        if d.get("lang") == "en":
            description = d.get("value", "")
            break
    if not description:
        return None

    # Get CVSS score — prefer v3.1 > v3.0 > v2
    cvss_score = 0.0
    for key in ("cvssMetricV31", "cvssMetricV30", "cvssMetricV2"):
        if key in metrics and metrics[key]:
            data = metrics[key][0]
            cvss_score = (
                data.get("cvssData", {}).get("baseScore") or
                data.get("baseScore") or 0.0
            )
            if cvss_score:
                break

    if cvss_score < _CVSS_MIN:
        return None

    # Must be Linux-related
    desc_lower = description.lower()
    if not any(kw in desc_lower for kw in NVD_KEYWORD_FILTERS):
        return None

    # CPE list for component + version extraction
    cpe_list: list[str] = []
    if "nodes" in vulns:
        for node in vulns.get("nodes", []):
            for match in node.get("cpeMatch", []):
                if match.get("vulnerable"):
                    cpe_list.append(match.get("criteria", ""))

    component     = _extract_nvd_component(description, cpe_list)
    version_scope = _extract_nvd_version_scope(cpe_list)
    kernel        = extract_kernel(description + " " + " ".join(cpe_list))

    # Failure type from CWE
    failure_type = "other"
    for w in weaknesses:
        for wd in w.get("description", []):
            cwe = wd.get("value", "")
            if cwe in _CWE_TO_FAILURE:
                failure_type = _CWE_TO_FAILURE[cwe]
                break

    # Solution: look for patch/advisory references
    patch_refs = [
        r["url"] for r in refs
        if any(kw in r.get("url", "").lower()
               for kw in ["patch", "commit", "fix", "advisory", "announce"])
    ]
    solution = ""
    if patch_refs:
        solution = f"Patch/fix references:\n" + "\n".join(patch_refs[:5])
    else:
        solution = f"Apply vendor security update for {component}. See CVE advisory."

    # raw_logs: CVE description serves as the "log" for security entries
    raw_logs = (
        f"CVE ID: {cve_id}\n"
        f"CVSS Score: {cvss_score}\n"
        f"Description: {description}\n"
        f"Affected CPEs: {'; '.join(cpe_list[:5])}"
    )

    link = f"https://nvd.nist.gov/vuln/detail/{cve_id}"

    doc = LinuxLynxDoc.build(
        doc_id        = f"nvd_{cve_id.replace('-', '_')}",
        source        = "web",
        domain        = "security",
        failure_type  = failure_type,
        distro        = "unknown",
        kernel        = kernel,
        component     = component,
        problem       = f"{cve_id}: {description[:200]}",
        raw_logs      = raw_logs,
        debug_steps   = "",
        root_cause    = description[:500],
        solution      = solution,
        reasoning     = (
            f"CVSS {cvss_score} severity {failure_type} vulnerability in {component}. "
            f"Applying the referenced patch removes the vulnerable code path."
        ),
        version_scope = version_scope,
        confidence    = "high",  # NVD = authoritative source
        link          = link,
    )

    errs = doc.validate()
    if errs:
        log.debug("CVE %s validation errors: %s", cve_id, errs)
        return None
    return doc


def scrape_nvd(
    keywords: list[str] = NVD_KEYWORD_FILTERS,
    max_docs: int = MAX_CVE,
    start_index: int = 0,
    api_key: str | None = None,
) -> Generator[LinuxLynxDoc, None, None]:
    """Yield security LinuxLynxDoc from NVD CVE database."""
    total = 0
    extra_headers = {}
    if api_key:
        extra_headers["apiKey"] = api_key

    for keyword in keywords:
        if total >= max_docs:
            break
        offset = 0

        while total < max_docs:
            params = {
                "keywordSearch": keyword,
                "startIndex":    offset,
                "resultsPerPage": 20,
                "noRejected":    "",
            }
            if api_key:
                params["apiKey"] = api_key

            log.info("NVD: keyword=%r offset=%d", keyword, offset)
            data = _nvd_get(params)
            if not data:
                break

            vulns = data.get("vulnerabilities", [])
            if not vulns:
                break

            for v in vulns:
                cve_item = v.get("cve", {})
                doc = _build_nvd_doc(cve_item)
                if doc:
                    total += 1
                    yield doc
                    if total >= max_docs:
                        return
                time.sleep(0.1)

            offset += 20
            time.sleep(NVD_DELAY)

            if offset >= data.get("totalResults", 0):
                break


# ══════════════════════════════════════════════════════════════════════════════
# Part 2: Syzkaller crash scraper
# ══════════════════════════════════════════════════════════════════════════════

SYZBOT_BASE     = "https://syzkaller.appspot.com"
SYZBOT_BUGS_URL = f"{SYZBOT_BASE}/upstream/fixed"   # Fixed bugs only
MAX_SYZBOT      = 150


def _syz_get(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning("Syzbot GET %s failed (attempt %d): %s", url, attempt + 1, e)
            time.sleep(DELAY * (attempt + 1))
    return None


def _parse_syzbot_bug(bug_url: str) -> LinuxLynxDoc | None:
    """Parse a single syzbot bug page."""
    r = _syz_get(bug_url)
    if not r:
        return None

    soup    = BeautifulSoup(r.text, "html.parser")
    title   = soup.find("title")
    title   = title.get_text(strip=True) if title else "Unknown kernel crash"
    title   = re.sub(r"\s*[-|]\s*syzbot.*$", "", title, flags=re.I).strip()

    # Bug report text — syzbot shows crash logs in <pre> blocks
    pre_blocks = soup.find_all("pre")
    raw_logs   = ""
    for pre in pre_blocks[:3]:
        text = pre.get_text()
        if len(text.strip()) > 50:
            raw_logs += text.strip() + "\n---\n"
    raw_logs = raw_logs[:3000].strip()

    if len(raw_logs) < 50:
        return None

    # Find fix commit link
    fix_commit  = ""
    commit_link = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "git.kernel.org" in href or "github.com/torvalds" in href:
            if "commit" in href:
                commit_link = href
                fix_commit  = a.get_text(strip=True)
                break

    # Tags / labels
    all_text = soup.get_text()

    kernel    = extract_kernel(raw_logs + " " + all_text)
    component = extract_component(raw_logs + " " + title)

    # Failure type from title/log
    failure_type = "other"
    title_lower  = title.lower()
    log_lower    = raw_logs.lower()
    if "null pointer" in title_lower or "null-ptr" in title_lower:
        failure_type = "segfault"
    elif "use-after-free" in title_lower or "uaf" in title_lower:
        failure_type = "segfault"
    elif "panic" in title_lower or "panic" in log_lower:
        failure_type = "kernel panic"
    elif "deadlock" in title_lower:
        failure_type = "other"
    elif "out-of-bounds" in title_lower or "oob" in title_lower:
        failure_type = "segfault"

    solution = (
        f"Fix commit: {fix_commit}\n{commit_link}"
        if fix_commit else
        "See syzbot bug page for fix details."
    )

    uid = hashlib.md5(bug_url.encode()).hexdigest()[:12]

    doc = LinuxLynxDoc.build(
        doc_id        = f"syzkaller_{uid}",
        source        = "bugzilla",   # treat as bugzilla-equivalent
        domain        = "kernel",
        failure_type  = failure_type,
        distro        = "unknown",
        kernel        = kernel,
        component     = component,
        problem       = title,
        raw_logs      = raw_logs[:2000],
        debug_steps   = "Reproduced by syzkaller fuzzer; repro program available on bug page.",
        root_cause    = f"Kernel crash triggered by syzkaller fuzzer: {title}",
        solution      = solution,
        reasoning     = (
            "Syzkaller found a reproducible crash path. "
            "Applying the linked fix commit removes the crash trigger."
        ),
        version_scope = kernel if kernel != "unknown" else "unknown",
        confidence    = "high" if fix_commit else "medium",
        link          = bug_url,
    )

    errs = doc.validate()
    if errs:
        log.debug("Syzbot %s validation: %s", bug_url, errs)
        return None

    return doc


def scrape_syzkaller(max_docs: int = MAX_SYZBOT) -> Generator[LinuxLynxDoc, None, None]:
    """Yield kernel crash LinuxLynxDoc from syzkaller fixed bugs."""
    r = _syz_get(SYZBOT_BUGS_URL)
    if not r:
        log.error("Cannot fetch syzbot fixed bugs page")
        return

    soup  = BeautifulSoup(r.text, "html.parser")
    total = 0

    # Bug links in the table
    for a in soup.find_all("a", href=True):
        if total >= max_docs:
            break
        href = a["href"]
        # syzbot bug URLs look like /bug?id=XXXX or /upstream/bug?id=XXXX
        if "bug?id=" not in href and "/bug/" not in href:
            continue
        bug_url = urljoin(SYZBOT_BASE, href)

        log.info("Syzbot: processing %s", bug_url)
        time.sleep(DELAY)

        doc = _parse_syzbot_bug(bug_url)
        if doc:
            total += 1
            log.info("  → accepted %s: %s", doc.doc_id, doc.problem[:60])
            yield doc


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    from dotenv import load_dotenv
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator
    
    load_dotenv()
    deduper = Deduplicator()

    mode = sys.argv[1] if len(sys.argv) > 1 else "nvd"
    out  = sys.argv[2] if len(sys.argv) > 2 else f"{mode}.jsonl"

    count = 0
    with open(out, "w") as fh:
        if mode == "nvd":
            api_key = os.getenv("NVD_API_KEY")
            gen = scrape_nvd(max_docs=5, api_key=api_key)
        else:
            gen = scrape_syzkaller(max_docs=5)

        for doc in gen:
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if not deduper.is_duplicate(content_to_hash):
                fh.write(doc.to_jsonl() + "\n")
                count += 1
                print(f"[{count}] NEW: {doc.doc_id}: {doc.problem[:70]}")
            else:
                print(f"[-] DUP: {doc.doc_id}")
        deduper._save_hashes()

    print(f"\nWrote {count} docs → {out}")
