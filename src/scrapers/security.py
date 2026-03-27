"""
Security data scrapers.

1. NVD (National Vulnerability Database)
   Maps CVE records to LinuxLynxDoc with domain=security.
   Only includes Linux-affecting CVEs with CVSS score >= 6.0.

2. Syzkaller
   Kernel crash reproducers from continuous fuzzing.
   Includes raw crash logs, repro programs, and status (open/fixed).
   Only fetches bugs with status=fixed.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import sys
import time
from typing import Generator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.schema import LinuxLynxDoc, extract_component, extract_kernel

log = logging.getLogger(__name__)

DELAY = 1.5

HEADERS = {
    "User-Agent": "LinuxLynx-DataCollector/1.0 (research; contact: dataset@linuxlynx.dev)"
}


# Part 1: NVD CVE scraper

NVD_API = "https://services.nvd.nist.gov/rest/json/cves/2.0"
NVD_DELAY_NO_KEY = 6.0
NVD_DELAY_WITH_KEY = 0.7
NVD_PAGE_SIZE = 500
MAX_CVE = 200

NVD_KEYWORD_FILTERS = [
    "linux kernel",
    "systemd",
    "glibc",
    "openssh",
    "sudo",
    "grub",
    "nfs",
    "ext4",
    "btrfs",
    "iptables",
    "nftables",
    "docker",
    "podman",
    "qemu",
    "kvm",
]

_NVD_TARGET_PATTERNS = [
    re.compile(r"\blinux kernel\b", re.I),
    re.compile(r"\bsystemd(?:[\s-]|$)", re.I),
    re.compile(r"\bglibc\b", re.I),
    re.compile(r"\bopenssh\b", re.I),
    re.compile(r"\bsudo\b", re.I),
    re.compile(r"\bgrub(?:2)?\b", re.I),
    re.compile(r"\bnfs\b", re.I),
    re.compile(r"\bext4\b", re.I),
    re.compile(r"\bbtrfs\b", re.I),
    re.compile(r"\biptables\b", re.I),
    re.compile(r"\bnftables\b", re.I),
    re.compile(r"\bdocker\b", re.I),
    re.compile(r"\bpodman\b", re.I),
    re.compile(r"\bqemu\b", re.I),
    re.compile(r"\bkvm\b", re.I),
]

_CVSS_MIN = 6.0

_CWE_TO_FAILURE = {
    "CWE-119": "segfault",
    "CWE-120": "segfault",
    "CWE-125": "segfault",
    "CWE-787": "segfault",
    "CWE-416": "segfault",
    "CWE-476": "segfault",
    "CWE-269": "permission",
    "CWE-284": "permission",
    "CWE-400": "other",
    "CWE-362": "other",
    "CWE-20": "other",
}


def _nvd_get(params: dict, api_key: str | None = None, retries: int = 3) -> dict | None:
    request_headers = dict(HEADERS)
    if api_key:
        request_headers["apiKey"] = api_key

    for attempt in range(retries):
        try:
            response = requests.get(NVD_API, params=params, headers=request_headers, timeout=30)
            if response.status_code == 403:
                wait_seconds = 5 if api_key else 30
                log.warning("NVD rate limit hit, waiting %ss...", wait_seconds)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            log.warning("NVD GET failed (attempt %d): %s", attempt + 1, exc)
            base_delay = NVD_DELAY_WITH_KEY if api_key else NVD_DELAY_NO_KEY
            time.sleep(base_delay * (attempt + 1))
    return None


def _extract_nvd_component(description: str, cpe_list: list[str]) -> str:
    for cpe in cpe_list[:3]:
        parts = cpe.split(":")
        if len(parts) >= 5:
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


def _iter_cpe_matches(nodes: list[dict]) -> list[str]:
    criteria: list[str] = []
    for node in nodes or []:
        for match in node.get("cpeMatch", []):
            if match.get("vulnerable") and match.get("criteria"):
                criteria.append(match["criteria"])
        children = node.get("nodes", [])
        if children:
            criteria.extend(_iter_cpe_matches(children))
    return criteria


def _extract_vulnerable_cpes(configurations: list[dict] | dict) -> list[str]:
    if isinstance(configurations, dict):
        return _iter_cpe_matches(configurations.get("nodes", []))

    cpe_list: list[str] = []
    for config in configurations or []:
        cpe_list.extend(_iter_cpe_matches(config.get("nodes", [])))
    return cpe_list


def _is_linux_related(description: str, cpe_list: list[str]) -> bool:
    searchable_text = f"{description} {' '.join(cpe_list)}".replace("_", " ")
    return any(pattern.search(searchable_text) for pattern in _NVD_TARGET_PATTERNS)


def _build_nvd_doc(item: dict) -> LinuxLynxDoc | None:
    cve_id = item.get("id", "")
    metrics = item.get("metrics", {})
    descriptions = item.get("descriptions", [])
    configurations = item.get("configurations", [])
    references = item.get("references", [])
    weaknesses = item.get("weaknesses", [])

    description = ""
    for entry in descriptions:
        if entry.get("lang") == "en":
            description = entry.get("value", "")
            break
    if not description:
        return None

    cvss_score = 0.0
    for key in ("cvssMetricV31", "cvssMetricV30", "cvssMetricV2"):
        if key in metrics and metrics[key]:
            data = metrics[key][0]
            cvss_score = data.get("cvssData", {}).get("baseScore") or data.get("baseScore") or 0.0
            if cvss_score:
                break

    if cvss_score < _CVSS_MIN:
        return None

    cpe_list = _extract_vulnerable_cpes(configurations)
    if not _is_linux_related(description, cpe_list):
        return None

    component = _extract_nvd_component(description, cpe_list)
    version_scope = _extract_nvd_version_scope(cpe_list)
    kernel = extract_kernel(description + " " + " ".join(cpe_list))

    failure_type = "other"
    for weakness in weaknesses:
        for weak_desc in weakness.get("description", []):
            cwe = weak_desc.get("value", "")
            if cwe in _CWE_TO_FAILURE:
                failure_type = _CWE_TO_FAILURE[cwe]
                break

    patch_refs = [
        ref["url"]
        for ref in references
        if any(keyword in ref.get("url", "").lower() for keyword in ["patch", "commit", "fix", "advisory", "announce"])
    ]
    if patch_refs:
        solution = "Patch/fix references:\n" + "\n".join(patch_refs[:5])
    else:
        solution = f"Apply vendor security update for {component}. See CVE advisory."

    raw_logs = (
        f"CVE ID: {cve_id}\n"
        f"CVSS Score: {cvss_score}\n"
        f"Description: {description}\n"
        f"Affected CPEs: {'; '.join(cpe_list[:5])}"
    )

    doc = LinuxLynxDoc.build(
        doc_id=f"nvd_{cve_id.replace('-', '_')}",
        source="web",
        domain="security",
        failure_type=failure_type,
        distro="unknown",
        kernel=kernel,
        component=component,
        problem=f"{cve_id}: {description[:200]}",
        raw_logs=raw_logs,
        debug_steps="",
        root_cause=description[:500],
        solution=solution,
        reasoning=(
            f"CVSS {cvss_score} severity {failure_type} vulnerability in {component}. "
            "Applying the referenced patch removes the vulnerable code path."
        ),
        version_scope=version_scope,
        confidence="high",
        link=f"https://nvd.nist.gov/vuln/detail/{cve_id}",
    )

    errors = doc.validate()
    if errors:
        log.debug("CVE %s validation errors: %s", cve_id, errors)
        return None
    return doc


def scrape_nvd(
    keywords: list[str] = NVD_KEYWORD_FILTERS,
    max_docs: int = MAX_CVE,
    start_index: int = 0,
    api_key: str | None = None,
) -> Generator[LinuxLynxDoc, None, None]:
    """Yield security LinuxLynxDoc from the NVD CVE database."""
    total = 0
    seen_ids: set[str] = set()
    page_size = min(NVD_PAGE_SIZE, max(100, max_docs * 2))
    delay = NVD_DELAY_WITH_KEY if api_key else NVD_DELAY_NO_KEY

    for keyword in keywords:
        if total >= max_docs:
            break

        probe = _nvd_get(
            {
                "keywordSearch": keyword,
                "startIndex": start_index,
                "resultsPerPage": 1,
            },
            api_key=api_key,
        )
        if not probe:
            continue

        total_results = int(probe.get("totalResults", 0))
        if total_results <= 0:
            continue

        offset = max(total_results - page_size, 0)
        visited_offsets: set[int] = set()

        while total < max_docs and offset not in visited_offsets:
            visited_offsets.add(offset)
            params = {
                "keywordSearch": keyword,
                "startIndex": offset,
                "resultsPerPage": page_size,
            }

            log.info("NVD: keyword=%r offset=%d", keyword, offset)
            data = _nvd_get(params, api_key=api_key)
            if not data:
                break

            vulnerabilities = data.get("vulnerabilities", [])
            if not vulnerabilities:
                break

            for vulnerability in reversed(vulnerabilities):
                cve_item = vulnerability.get("cve", {})
                cve_id = cve_item.get("id", "")
                if not cve_id or cve_id in seen_ids:
                    continue
                doc = _build_nvd_doc(cve_item)
                if not doc:
                    continue
                seen_ids.add(cve_id)
                total += 1
                yield doc
                if total >= max_docs:
                    return

            if offset == 0:
                break

            offset = max(offset - page_size, 0)
            time.sleep(delay)


# Part 2: Syzkaller crash scraper

SYZBOT_BASE = "https://syzkaller.appspot.com"
SYZBOT_BUGS_URL = f"{SYZBOT_BASE}/upstream/fixed"
MAX_SYZBOT = 150


def _syz_get(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=25)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            log.warning("Syzbot GET %s failed (attempt %d): %s", url, attempt + 1, exc)
            time.sleep(DELAY * (attempt + 1))
    return None


def _parse_syzbot_bug(bug_url: str) -> LinuxLynxDoc | None:
    """Parse a single syzbot bug page."""
    response = _syz_get(bug_url)
    if not response:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.find("title")
    title = title.get_text(strip=True) if title else "Unknown kernel crash"
    title = re.sub(r"\s*[-|]\s*syzbot.*$", "", title, flags=re.I).strip()

    pre_blocks = soup.find_all("pre")
    raw_logs = ""
    for pre in pre_blocks[:3]:
        text = pre.get_text()
        if len(text.strip()) > 50:
            raw_logs += text.strip() + "\n---\n"
    raw_logs = raw_logs[:3000].strip()

    if len(raw_logs) < 50:
        raw_logs = soup.get_text(separator="\n")
        raw_logs = "\n".join(
            line
            for line in raw_logs.splitlines()
            if re.search(r"KASAN|panic|BUG|WARNING|Oops|crash|fault|null|use-after|oob|stack", line, re.I)
        )[:2000].strip()

    if len(raw_logs) < 50:
        return None

    fix_commit = ""
    commit_link = ""
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if ("git.kernel.org" in href or "github.com/torvalds" in href) and "commit" in href:
            commit_link = href
            fix_commit = anchor.get_text(strip=True)
            break

    all_text = soup.get_text()
    kernel = extract_kernel(raw_logs + " " + all_text)
    component = extract_component(raw_logs + " " + title)

    failure_type = "other"
    title_lower = title.lower()
    log_lower = raw_logs.lower()
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

    if fix_commit:
        solution = f"Fix commit: {fix_commit}\n{commit_link}"
    else:
        solution = "See syzbot bug page for fix details."

    uid = hashlib.md5(bug_url.encode()).hexdigest()[:12]

    doc = LinuxLynxDoc.build(
        doc_id=f"syzkaller_{uid}",
        source="bugzilla",
        domain="kernel",
        failure_type=failure_type,
        distro="unknown",
        kernel=kernel,
        component=component,
        problem=title,
        raw_logs=raw_logs[:2000],
        debug_steps="Reproduced by syzkaller fuzzer; repro program available on bug page.",
        root_cause=f"Kernel crash triggered by syzkaller fuzzer: {title}",
        solution=solution,
        reasoning=(
            "Syzkaller found a reproducible crash path. "
            "Applying the linked fix commit removes the crash trigger."
        ),
        version_scope=kernel if kernel != "unknown" else "unknown",
        confidence="high" if fix_commit else "medium",
        link=bug_url,
    )

    errors = doc.validate()
    if errors:
        log.debug("Syzbot %s validation: %s", bug_url, errors)
        return None

    return doc


def scrape_syzkaller(max_docs: int = MAX_SYZBOT) -> Generator[LinuxLynxDoc, None, None]:
    """Yield kernel crash LinuxLynxDoc from syzkaller fixed bugs."""
    response = _syz_get(SYZBOT_BUGS_URL)
    if not response:
        log.error("Cannot fetch syzbot fixed bugs page")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    total = 0

    for anchor in soup.find_all("a", href=True):
        if total >= max_docs:
            break
        href = anchor["href"]
        if "bug?id=" not in href and "/bug/" not in href:
            continue

        bug_url = urljoin(SYZBOT_BASE, href)
        log.info("Syzbot: processing %s", bug_url)
        time.sleep(DELAY)

        doc = _parse_syzbot_bug(bug_url)
        if doc:
            total += 1
            log.info("  -> accepted %s: %s", doc.doc_id, doc.problem[:60])
            yield doc


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from src.dedup import Deduplicator

    load_dotenv()
    deduper = Deduplicator()

    mode = sys.argv[1] if len(sys.argv) > 1 else "nvd"
    out = sys.argv[2] if len(sys.argv) > 2 else f"{mode}.jsonl"

    count = 0
    with open(out, "w", encoding="utf-8") as handle:
        if mode == "nvd":
            api_key = os.getenv("NVD_API_KEY")
            generator = scrape_nvd(max_docs=5, api_key=api_key)
        else:
            generator = scrape_syzkaller(max_docs=5)

        for doc in generator:
            content_to_hash = doc.problem + doc.raw_logs + doc.solution
            if not deduper.is_duplicate(content_to_hash):
                handle.write(doc.to_jsonl() + "\n")
                count += 1
                print(f"[{count}] NEW: {doc.doc_id}: {doc.problem[:70]}")
            else:
                print(f"[-] DUP: {doc.doc_id}")
        deduper._save_hashes()

    print(f"\nWrote {count} docs -> {out}")
