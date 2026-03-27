"""
Crawl4AI-based async scraper for Linux RAG data.
Supports: lkml.org, lore.kernel.org, lists.debian.org, lists.ubuntu.com,
          linuxquestions.org, bbs.archlinux.org, ubuntuforums.org,
          + man7.org, wiki.archlinux.org, debian.org wiki
"""

import asyncio
import json
import hashlib
import re
import argparse
import os
import sys
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Optional
from dataclasses import dataclass, asdict

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
    from crawl4ai.extraction_strategy import JsonCssExtractionStrategy, LLMExtractionStrategy
    from crawl4ai import DeepCrawlStrategy
    from crawl4ai.content_filter_strategy import PruningContentFilter
    from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
    HAS_CRAWL4AI = True
except ImportError as e:
    print("REAL ERROR:", e)
    sys.exit(1)


# ─── helpers ────────────────────────────────────────────────────────────────

def make_doc_id(url: str) -> str:
    return "web_" + hashlib.md5(url.encode()).hexdigest()[:12]


DOMAIN_KEYWORDS = {
    "kernel/boot":        ["kernel", "boot", "grub", "initramfs", "vmlinuz", "bzimage", "kexec"],
    "kernel/drivers":     ["driver", "module", "firmware", "dkms", "udev", "modprobe", "lsmod"],
    "networking":         ["network", "iptables", "firewall", "wifi", "ethernet", "dns", "dhcp",
                           "ip route", "nftables", "wireguard", "openvpn", "networkmanager"],
    "filesystem":         ["filesystem", "ext4", "btrfs", "zfs", "xfs", "mount", "fstab",
                           "partition", "lvm", "raid", "mkfs", "fsck", "tmpfs"],
    "systemd":            ["systemd", "systemctl", "journalctl", "unit file", "service",
                           "timer", "socket", "target", "daemon"],
    "package-management": ["apt", "dpkg", "pacman", "dnf", "yum", "rpm", "pip",
                           "snap", "flatpak", "nix", "portage", "emerge"],
    "security":           ["selinux", "apparmor", "sudo", "permission", "chmod", "chown",
                           "ssl", "tls", "gpg", "ssh", "ufw", "fail2ban", "pam"],
    "display/graphics":   ["xorg", "wayland", "gpu", "nvidia", "amdgpu", "display",
                           "screen", "drm", "kms", "framebuffer", "compositor"],
    "audio":              ["alsa", "pulseaudio", "pipewire", "jack", "sound", "audio",
                           "snd_", "amixer", "pavucontrol"],
    "virtualization":     ["docker", "podman", "kvm", "qemu", "virtualbox", "container",
                           "lxc", "lxd", "namespace", "cgroup", "hypervisor"],
    "shell/scripting":    ["bash", "shell", "script", "zsh", "fish", "cron", "awk",
                           "sed", "grep", "regex", "pipe", "redirect"],
    "hardware":           ["cpu", "memory", "ram", "disk", "ssd", "nvme", "pcie",
                           "usb", "acpi", "power", "thermal", "fan", "sensor"],
    "init/startup":       ["rc.local", "init.d", "runlevel", "openrc", "runit", "s6"],
    "performance":        ["perf", "ftrace", "ebpf", "bpf", "profiling", "latency",
                           "tuning", "cpufreq", "irq", "affinity"],
}


def detect_domain(url: str, text: str = "") -> str:
    combined = (url + " " + text[:600]).lower()
    for domain, kws in DOMAIN_KEYWORDS.items():
        if any(kw in combined for kw in kws):
            return domain
    return "general/linux"


HW_PATTERNS = [
    "arch", "ubuntu", "debian", "fedora", "centos", "rhel", "opensuse",
    "gentoo", "manjaro", "mint", "kali", "alpine", "nixos", "void",
    "systemd", "openrc", "sysvinit", "runit",
    "gnome", "kde", "xfce", "i3", "sway", "hyprland", "plasma", "cinnamon",
    "x86_64", "arm64", "aarch64", "riscv", "i686",
    "linux-lts", "linux-zen", "linux-hardened",
    "wayland", "x11", "xorg",
]


def detect_hw_env(text: str) -> str:
    found = [p for p in HW_PATTERNS if p in text.lower()]
    return ", ".join(dict.fromkeys(found)[:6]) or "linux"


SOLUTION_RE = re.compile(
    r"(?:solved|solution|fix|fixed|resolved|workaround|the answer is|turns out)[:\s]+(.{20,400})",
    re.IGNORECASE | re.DOTALL,
)

COMMAND_RE = re.compile(r"(?:^|\n)\s*(?:\$|#|%)\s+(.+)", re.MULTILINE)


def extract_solution(text: str) -> str:
    m = SOLUTION_RE.search(text)
    if m:
        return m.group(1).strip()[:500]
    return "Unresolved"


def extract_raw_logs(text: str) -> str:
    """Pull out command lines and error/log snippets."""
    lines = []
    # Command lines
    for m in COMMAND_RE.finditer(text):
        lines.append(m.group(1).strip())
    # Common log patterns
    for m in re.finditer(r"(?:error|warning|failed|denied|panic|oops|trace)[^\n]{0,200}", text, re.IGNORECASE):
        lines.append(m.group(0).strip())
    return "\n".join(lines[:30])


@dataclass
class LinuxDoc:
    doc_id: str
    domain: str
    hardware_env: str
    problem: str
    raw_logs: str
    solution: str
    source_file: str
    original_link: str


# ─── site configs ────────────────────────────────────────────────────────────

SITE_CONFIGS = {
    "lkml": {
        "seeds": ["https://lkml.org/lkml/"],
        "url_pattern": r"lkml\.org/lkml/\d{4}/\d+/\d+",
        "nav_pattern": r"lkml\.org/lkml/(\d{4}/?|/\d{4}/\d+/?)$",
        "max_pages": 100000,
        "depth": 6,
        "output": "lkml_crawl4ai.jsonl",
    },
    "lore_kernel": {
        "seeds": [
            "https://lore.kernel.org/linux-kernel/",
            "https://lore.kernel.org/linux-block/",
            "https://lore.kernel.org/linux-mm/",
            "https://lore.kernel.org/linux-net/",
            "https://lore.kernel.org/linux-fs/",
            "https://lore.kernel.org/linux-security-module/",
            "https://lore.kernel.org/linux-wireless/",
            "https://lore.kernel.org/linux-sound/",
            "https://lore.kernel.org/linux-acpi/",
            "https://lore.kernel.org/linux-usb/",
            "https://lore.kernel.org/linux-input/",
            "https://lore.kernel.org/linux-arm-kernel/",
        ],
        "url_pattern": r"lore\.kernel\.org/.+@.+",
        "nav_pattern": r"lore\.kernel\.org/[^/]+/\?o=\d+",
        "max_pages": 150000,
        "depth": 5,
        "output": "lore_kernel_crawl4ai.jsonl",
    },
    "debian_lists": {
        "seeds": [
            "https://lists.debian.org/debian-user/",
            "https://lists.debian.org/debian-kernel/",
            "https://lists.debian.org/debian-devel/",
            "https://lists.debian.org/debian-security/",
            "https://lists.debian.org/debian-hardware/",
            "https://lists.debian.org/debian-bugs-dist/",
            "https://lists.debian.org/debian-embedded/",
            "https://lists.debian.org/debian-arm/",
            "https://lists.debian.org/debian-x/",
        ],
        "url_pattern": r"lists\.debian\.org/.+/msg\d+\.html",
        "nav_pattern": r"lists\.debian\.org/[^/]+/\d{4}/\d+/",
        "max_pages": 80000,
        "depth": 5,
        "output": "debian_lists_crawl4ai.jsonl",
    },
    "ubuntu_lists": {
        "seeds": [
            "https://lists.ubuntu.com/archives/ubuntu-users/",
            "https://lists.ubuntu.com/archives/ubuntu-devel/",
            "https://lists.ubuntu.com/archives/ubuntu-kernel-team/",
            "https://lists.ubuntu.com/archives/ubuntu-bugs/",
            "https://lists.ubuntu.com/archives/ubuntu-server/",
            "https://lists.ubuntu.com/archives/ubuntu-desktop/",
        ],
        "url_pattern": r"lists\.ubuntu\.com/archives/.+/\d+\.html",
        "nav_pattern": r"lists\.ubuntu\.com/archives/[^/]+/\d{4}-\w+",
        "max_pages": 60000,
        "depth": 5,
        "output": "ubuntu_lists_crawl4ai.jsonl",
    },
    "linux_questions": {
        "seeds": [
            "https://www.linuxquestions.org/questions/linux-general-1/",
            "https://www.linuxquestions.org/questions/linux-kernel-70/",
            "https://www.linuxquestions.org/questions/linux-networking-3/",
            "https://www.linuxquestions.org/questions/linux-hardware-18/",
            "https://www.linuxquestions.org/questions/linux-software-2/",
            "https://www.linuxquestions.org/questions/linux-server-73/",
            "https://www.linuxquestions.org/questions/linux-security-4/",
            "https://www.linuxquestions.org/questions/debian-26/",
            "https://www.linuxquestions.org/questions/ubuntu-92/",
            "https://www.linuxquestions.org/questions/arch-linux-91/",
            "https://www.linuxquestions.org/questions/fedora-35/",
        ],
        "url_pattern": r"linuxquestions\.org/questions/[^/]+-\d+/[^/]+-\d+",
        "max_pages": 100000,
        "depth": 5,
        "output": "linuxquestions_crawl4ai.jsonl",
    },
    "arch_forums": {
        "seeds": ["https://bbs.archlinux.org/index.php"],
        "url_pattern": r"bbs\.archlinux\.org/viewtopic\.php",
        "nav_pattern": r"bbs\.archlinux\.org/viewforum\.php",
        "max_pages": 80000,
        "depth": 6,
        "output": "arch_forums_crawl4ai.jsonl",
    },
    "ubuntu_forums": {
        "seeds": [
            "https://ubuntuforums.org/forumdisplay.php?f=48",
            "https://ubuntuforums.org/forumdisplay.php?f=333",
            "https://ubuntuforums.org/forumdisplay.php?f=338",
            "https://ubuntuforums.org/forumdisplay.php?f=336",
            "https://ubuntuforums.org/forumdisplay.php?f=100",
        ],
        "url_pattern": r"ubuntuforums\.org/showthread\.php",
        "max_pages": 80000,
        "depth": 5,
        "output": "ubuntu_forums_crawl4ai.jsonl",
    },
    # Bonus high-quality sources
    "arch_wiki": {
        "seeds": ["https://wiki.archlinux.org/title/Special:AllPages"],
        "url_pattern": r"wiki\.archlinux\.org/title/[^:]+$",
        "max_pages": 10000,
        "depth": 3,
        "output": "arch_wiki_crawl4ai.jsonl",
    },
    "man7": {
        "seeds": [
            "https://man7.org/linux/man-pages/dir_all_alphabetic.html",
        ],
        "url_pattern": r"man7\.org/linux/man-pages/man\d/",
        "max_pages": 5000,
        "depth": 3,
        "output": "man7_crawl4ai.jsonl",
    },
    "kernel_docs": {
        "seeds": ["https://docs.kernel.org/"],
        "url_pattern": r"docs\.kernel\.org/",
        "max_pages": 20000,
        "depth": 5,
        "output": "kernel_docs_crawl4ai.jsonl",
    },
}


# ─── extraction ──────────────────────────────────────────────────────────────

def parse_crawl_result(result, site_name: str) -> Optional[LinuxDoc]:
    """Convert a crawl4ai result into a LinuxDoc."""
    if not result or not result.success:
        return None
    
    url = result.url
    
    # Use markdown content if available, fallback to raw text
    text = ""

    if hasattr(result, "markdown") and result.markdown:
        md = result.markdown
        if hasattr(md, "raw_markdown") and md.raw_markdown:
            text = md.raw_markdown
        elif isinstance(md, str):
            text = md

    if not text and hasattr(result, "cleaned_html") and result.cleaned_html:
        text = re.sub(r"<[^>]+>", " ", result.cleaned_html)

    if not text and hasattr(result, "html") and result.html:
        text = re.sub(r"<[^>]+>", " ", result.html)

    text = re.sub(r"\s+", " ", text).strip()
    
    if len(text) < 30:
        return None

    # Extract title
    title = ""
    if hasattr(result, "metadata") and result.metadata:
        title = result.metadata.get("title", "") or ""

    problem_text = (title + " " + text[:1200]).strip()
    raw_logs = extract_raw_logs(text)
    solution = extract_solution(text)

    return LinuxDoc(
        doc_id=make_doc_id(url),
        domain=detect_domain(url, text),
        hardware_env=detect_hw_env(text),
        problem=problem_text[:2000],
        raw_logs=raw_logs[:1000],
        solution=solution,
        source_file="heuristically_parsed",
        original_link=url,
    )


# ─── crawlers ────────────────────────────────────────────────────────────────

async def crawl_site(site_name: str, config: dict, output_dir: str,
                     max_pages: Optional[int] = None, concurrency: int = 10):

    effective_max = max_pages or config["max_pages"]
    output_path = os.path.join(output_dir, config["output"])
    
    print(f"\n{'='*60}")
    print(f"Starting: {site_name}")
    print(f"Output:   {output_path}")
    print(f"Max pages: {effective_max:,}")
    print(f"{'='*60}")

    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        browser_type="chromium",
        extra_args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
        ],
    )

    seen_urls = set()
    docs_written = 0
    
    os.makedirs(output_dir, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f_out:
        async with AsyncWebCrawler(config=browser_config) as crawler:

            deep_config = CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                word_count_threshold=20,
                excluded_tags=["nav", "footer", "header", "aside", "script", "style"],
                exclude_external_links=True,
                markdown_generator=DefaultMarkdownGenerator(
                    content_filter=PruningContentFilter(threshold=0.3)
                ),
                deep_crawl_strategy=DeepCrawlStrategy(
                    strategy="bfs",
                    max_depth=config.get("depth", 5),
                    max_pages=effective_max,
                ),
                stream=True,
            )
            
            for seed_url in config["seeds"]:
                if docs_written >= effective_max:
                    break
                    
                try:
                    async for result in await crawler.arun(seed_url, config=deep_config):

                        # 🔍 DEBUG BLOCK 1
                        print("\n--- DEBUG ---")
                        print("URL:", result.url)
                        print("Success:", result.success)

                        # Extract text manually for debug
                        debug_text = ""

                        if hasattr(result, "markdown") and result.markdown:
                            if hasattr(result.markdown, "raw_markdown") and result.markdown.raw_markdown:
                                debug_text = result.markdown.raw_markdown
                            elif isinstance(result.markdown, str):
                                debug_text = result.markdown

                        if not debug_text and hasattr(result, "cleaned_html") and result.cleaned_html:
                            debug_text = result.cleaned_html

                        print("Text length:", len(debug_text))
                        print("-------------")

                        if result.url in seen_urls:
                            continue
                        seen_urls.add(result.url)

                        # 🔧 FORCE BETTER TEXT EXTRACTION
                        text = ""

                        if hasattr(result, "markdown") and result.markdown:
                            if hasattr(result.markdown, "raw_markdown") and result.markdown.raw_markdown:
                                text = result.markdown.raw_markdown
                            elif isinstance(result.markdown, str):
                                text = result.markdown

                        if not text and hasattr(result, "cleaned_html") and result.cleaned_html:
                            text = re.sub(r"<[^>]+>", " ", result.cleaned_html)

                        if not text and hasattr(result, "html") and result.html:
                            text = re.sub(r"<[^>]+>", " ", result.html)

                        text = re.sub(r"\s+", " ", text).strip()

                        if len(text) < 30:
                            print("❌ DROPPED (too short):", result.url)
                            continue

                        # Extract metadata
                        title = ""
                        if hasattr(result, "metadata") and result.metadata:
                            title = result.metadata.get("title", "") or ""

                        problem_text = (title + " " + text[:1200]).strip()
                        raw_logs = extract_raw_logs(text)
                        solution = extract_solution(text)

                        doc = LinuxDoc(
                            doc_id=make_doc_id(result.url),
                            domain=detect_domain(result.url, text),
                            hardware_env=detect_hw_env(text),
                            problem=problem_text[:2000],
                            raw_logs=raw_logs[:1000],
                            solution=solution,
                            source_file="heuristically_parsed",
                            original_link=result.url,
                        )

                        # 🔍 DEBUG BLOCK 2
                        print("✅ SAVED:", result.url)

                        f_out.write(json.dumps(asdict(doc), ensure_ascii=False) + "\n")
                        docs_written += 1
                        
                        if docs_written % 200 == 0:
                            print(f"  [{site_name}] {docs_written:,} docs saved...")
                        
                        if docs_written >= effective_max:
                            print(f"  [{site_name}] Reached limit of {effective_max:,}")
                            break

                except Exception as e:
                    print(f"  [{site_name}] Error on {seed_url}: {e}")
                    continue
    
    print(f"  [{site_name}] DONE: {docs_written:,} docs → {output_path}")
    return docs_written

async def crawl_all(sites: list, output_dir: str, max_pages: Optional[int],
                    concurrency: int = 5):

    semaphore = asyncio.Semaphore(concurrency)
    
    async def run_with_sem(site_name, config):
        async with semaphore:
            return await crawl_site(site_name, config, output_dir, max_pages, concurrency)
    
    tasks = []
    for site_name in sites:
        if site_name not in SITE_CONFIGS:
            print(f"Unknown site: {site_name}")
            continue
        tasks.append(run_with_sem(site_name, SITE_CONFIGS[site_name]))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results
# ─── merge utility ───────────────────────────────────────────────────────────

def merge_outputs(output_dir: str, merged_file: str = "all_linux_data.jsonl"):
    """Merge all .jsonl files, dedup by doc_id."""
    seen = set()
    total = 0
    merged_path = os.path.join(output_dir, merged_file)
    
    with open(merged_path, "w", encoding="utf-8") as fout:
        for fname in os.listdir(output_dir):
            if not fname.endswith(".jsonl") or fname == merged_file:
                continue
            with open(os.path.join(output_dir, fname), "r", encoding="utf-8") as fin:
                for line in fin:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        doc_id = obj.get("doc_id", "")
                        if doc_id and doc_id not in seen:
                            seen.add(doc_id)
                            fout.write(line + "\n")
                            total += 1
                    except json.JSONDecodeError:
                        pass
    
    print(f"\nMerged {total:,} unique docs → {merged_path}")
    return total




# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Crawl4AI-based Linux RAG data scraper"
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        default=list(SITE_CONFIGS.keys()),
        choices=list(SITE_CONFIGS.keys()) + ["all"],
        help="Sites to crawl (default: all)",
    )
    parser.add_argument(
        "--output-dir",
        default="crawl4ai_output",
        help="Output directory for .jsonl files",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Override max pages per site (default: per-site limits)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of sites to crawl concurrently (default: 3)",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge all output files after crawling",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Only merge existing output files (skip crawling)",
    )
    
    args = parser.parse_args()
    
    sites = list(SITE_CONFIGS.keys()) if "all" in args.sites else args.sites
    
    if not args.merge_only:
        print(f"\n🚀 Starting Crawl4AI Linux scraper")
        print(f"   Sites: {', '.join(sites)}")
        print(f"   Output: {args.output_dir}/")
        print(f"   Max pages per site: {args.max_pages or 'site defaults'}")
        print(f"   Concurrency: {args.concurrency} sites in parallel\n")
        
        asyncio.run(crawl_all(
            sites=sites,
            output_dir=args.output_dir,
            max_pages=args.max_pages,
            concurrency=args.concurrency,
        ))
    
    if args.merge or args.merge_only:
        merge_outputs(args.output_dir)


if __name__ == "__main__":
    main()
