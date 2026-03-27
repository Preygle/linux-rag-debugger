import scrapy
import json
import hashlib
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse


def make_doc_id(url):
    return "web_" + hashlib.md5(url.encode()).hexdigest()[:12]


def detect_domain(url, text=""):
    """Heuristically detect the Linux domain from URL and content."""
    url_lower = url.lower()
    text_lower = text.lower()
    
    domain_hints = {
        "kernel/boot": ["kernel", "boot", "grub", "initramfs", "vmlinuz", "bzimage"],
        "kernel/drivers": ["driver", "module", "firmware", "dkms", "udev"],
        "networking": ["network", "iptables", "firewall", "wifi", "ethernet", "dns", "dhcp", "ip route"],
        "filesystem": ["filesystem", "ext4", "btrfs", "zfs", "mount", "fstab", "partition", "lvm"],
        "systemd": ["systemd", "systemctl", "journalctl", "service", "unit file"],
        "package-management": ["apt", "pacman", "dnf", "yum", "pip", "snap", "flatpak"],
        "security": ["selinux", "apparmor", "sudo", "permission", "chmod", "ssl", "gpg"],
        "display/graphics": ["xorg", "wayland", "gpu", "nvidia", "amdgpu", "display", "screen"],
        "audio": ["alsa", "pulseaudio", "pipewire", "sound", "audio"],
        "virtualization": ["docker", "kvm", "qemu", "virtualbox", "container", "lxc"],
        "shell/scripting": ["bash", "shell", "script", "zsh", "fish", "cron"],
        "hardware": ["cpu", "memory", "ram", "disk", "ssd", "nvme", "hardware"],
    }
    
    for domain, keywords in domain_hints.items():
        for kw in keywords:
            if kw in url_lower or kw in text_lower[:500]:
                return domain
    return "general/linux"


def detect_hw_env(text):
    """Detect hardware/software environment from text."""
    env_hints = []
    text_lower = text.lower()
    
    distros = ["arch", "ubuntu", "debian", "fedora", "centos", "rhel", "opensuse", "gentoo", "manjaro", "mint"]
    init_sys = ["systemd", "openrc", "sysvinit", "runit"]
    kernels = ["linux-lts", "linux-zen", "linux-hardened"]
    desktops = ["gnome", "kde", "xfce", "i3", "sway", "hyprland", "plasma"]
    archs = ["x86_64", "arm64", "aarch64", "riscv", "i686"]
    
    for item in distros + init_sys + kernels + desktops + archs:
        if item in text_lower:
            env_hints.append(item)
    
    return ", ".join(env_hints[:5]) if env_hints else "linux"


def extract_solution(text):
    """Try to extract a solution marker from text."""
    solution_patterns = [
        r"(?:solved|solution|fix|fixed|resolved|workaround)[:\s]+(.{20,300})",
        r"(?:you can|try|run|execute)[:\s]+(.{20,200})",
    ]
    for pat in solution_patterns:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(1).strip()[:500]
    return "Unresolved"


class LKMLSpider(scrapy.Spider):
    name = "lkml"
    allowed_domains = ["lkml.org"]
    start_urls = ["https://lkml.org/lkml/"]
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DEPTH_LIMIT": 6,
        "CLOSESPIDER_ITEMCOUNT": 50000,
        "FEEDS": {"lkml_output.jsonl": {"format": "jsonlines", "overwrite": True}},
    }

    def parse(self, response):
        # Parse thread list pages
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"/lkml/\d{4}/\d+/\d+", full):
                yield scrapy.Request(full, callback=self.parse_message)
            elif re.search(r"/lkml/\d{4}/?$", full) or re.search(r"/lkml/\d{4}/\d+/?$", full):
                yield scrapy.Request(full, callback=self.parse)

    def parse_message(self, response):
        title = response.css("h1::text, title::text").get("").strip()
        body = " ".join(response.css("pre::text, .body::text").getall()).strip()
        if not body:
            body = " ".join(response.css("p::text").getall()).strip()
        
        if len(body) < 50:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body),
            "hardware_env": detect_hw_env(body),
            "problem": (title + " " + body[:1000]).strip(),
            "raw_logs": "",
            "solution": extract_solution(body),
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }

        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"/lkml/\d{4}/\d+/\d+", full):
                yield scrapy.Request(full, callback=self.parse_message)


class LoreKernelSpider(scrapy.Spider):
    name = "lore_kernel"
    allowed_domains = ["lore.kernel.org"]
    start_urls = [
        "https://lore.kernel.org/linux-kernel/",
        "https://lore.kernel.org/linux-block/",
        "https://lore.kernel.org/linux-mm/",
        "https://lore.kernel.org/linux-net/",
        "https://lore.kernel.org/linux-usb/",
        "https://lore.kernel.org/linux-fs/",
        "https://lore.kernel.org/linux-security-module/",
        "https://lore.kernel.org/linux-wireless/",
        "https://lore.kernel.org/linux-input/",
        "https://lore.kernel.org/linux-sound/",
        "https://lore.kernel.org/linux-acpi/",
        "https://lore.kernel.org/linux-arm-kernel/",
        "https://lore.kernel.org/linux-graphics/",
    ]
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DEPTH_LIMIT": 5,
        "CLOSESPIDER_ITEMCOUNT": 80000,
        "FEEDS": {"lore_kernel_output.jsonl": {"format": "jsonlines", "overwrite": True}},
    }

    def parse(self, response):
        # Extract message links
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            # Message pages typically end with T/ or are individual thread links
            if re.search(r"lore\.kernel\.org/[^/]+/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", full):
                yield scrapy.Request(full, callback=self.parse_message)
            elif full.endswith("/?q=") or "?q=" in full:
                pass
            elif re.search(r"lore\.kernel\.org/[^/]+/\?o=\d+", full):
                yield scrapy.Request(full, callback=self.parse)

        # Pagination
        next_page = response.css("a[rel=next]::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse)

    def parse_message(self, response):
        title = response.css("h1::text, .subject::text").get("").strip()
        body = " ".join(response.css("pre::text").getall()).strip()
        
        if len(body) < 50:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body),
            "hardware_env": detect_hw_env(body),
            "problem": (title + " " + body[:1000]).strip(),
            "raw_logs": "",
            "solution": extract_solution(body),
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }


class DebianListsSpider(scrapy.Spider):
    name = "debian_lists"
    allowed_domains = ["lists.debian.org"]
    start_urls = [
        "https://lists.debian.org/debian-user/",
        "https://lists.debian.org/debian-kernel/",
        "https://lists.debian.org/debian-devel/",
        "https://lists.debian.org/debian-bugs-dist/",
        "https://lists.debian.org/debian-security/",
        "https://lists.debian.org/debian-laptop/",
        "https://lists.debian.org/debian-hardware/",
        "https://lists.debian.org/debian-embedded/",
        "https://lists.debian.org/debian-arm/",
        "https://lists.debian.org/debian-x/",
    ]
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DEPTH_LIMIT": 5,
        "CLOSESPIDER_ITEMCOUNT": 50000,
        "FEEDS": {"debian_lists_output.jsonl": {"format": "jsonlines", "overwrite": True}},
    }

    def parse(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"lists\.debian\.org/[^/]+/\d{4}/\d+/", full):
                yield scrapy.Request(full, callback=self.parse_thread_index)
            elif re.search(r"lists\.debian\.org/[^/]+/\d{4}/", full):
                yield scrapy.Request(full, callback=self.parse)

    def parse_thread_index(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"msg\d+\.html$", full):
                yield scrapy.Request(full, callback=self.parse_message)

    def parse_message(self, response):
        title = response.css("h1::text").get("").strip()
        body = " ".join(response.css("pre::text, .msgbody::text").getall()).strip()
        
        if len(body) < 50:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body),
            "hardware_env": detect_hw_env(body),
            "problem": (title + " " + body[:1000]).strip(),
            "raw_logs": "",
            "solution": extract_solution(body),
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }


class UbuntuListsSpider(scrapy.Spider):
    name = "ubuntu_lists"
    allowed_domains = ["lists.ubuntu.com"]
    start_urls = [
        "https://lists.ubuntu.com/archives/ubuntu-users/",
        "https://lists.ubuntu.com/archives/ubuntu-devel/",
        "https://lists.ubuntu.com/archives/ubuntu-kernel-team/",
        "https://lists.ubuntu.com/archives/ubuntu-bugs/",
        "https://lists.ubuntu.com/archives/ubuntu-security-announce/",
        "https://lists.ubuntu.com/archives/ubuntu-desktop/",
        "https://lists.ubuntu.com/archives/ubuntu-server/",
    ]
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DEPTH_LIMIT": 5,
        "CLOSESPIDER_ITEMCOUNT": 40000,
        "FEEDS": {"ubuntu_lists_output.jsonl": {"format": "jsonlines", "overwrite": True}},
    }

    def parse(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"\.txt\.gz$", full):
                pass  # skip archives
            elif re.search(r"/\d{4}-\w+/", full):
                yield scrapy.Request(full, callback=self.parse_month)

    def parse_month(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"\d+\.html$", full):
                yield scrapy.Request(full, callback=self.parse_message)

    def parse_message(self, response):
        title = response.css("h1::text").get("").strip()
        body = " ".join(response.css("pre::text").getall()).strip()
        
        if len(body) < 50:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body),
            "hardware_env": detect_hw_env(body),
            "problem": (title + " " + body[:1000]).strip(),
            "raw_logs": "",
            "solution": extract_solution(body),
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }


class LinuxQuestionsSpider(scrapy.Spider):
    name = "linux_questions"
    allowed_domains = ["www.linuxquestions.org"]
    start_urls = [
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
        "https://www.linuxquestions.org/questions/linux-desktop-74/",
        "https://www.linuxquestions.org/questions/linux-virtualization-90/",
        "https://www.linuxquestions.org/questions/linux-newbie-8/",
    ]
    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DEPTH_LIMIT": 5,
        "CLOSESPIDER_ITEMCOUNT": 80000,
        "FEEDS": {"linuxquestions_output.jsonl": {"format": "jsonlines", "overwrite": True}},
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        # Thread list page
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if re.search(r"/questions/[^/]+-\d+/[^/]+-\d+/", full) and "page" not in full.split("?")[-1]:
                yield scrapy.Request(full, callback=self.parse_thread)

        # Pagination
        next_page = response.css("a[rel='next']::attr(href), .next a::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse)

    def parse_thread(self, response):
        title = response.css("h1::text, .threadtitle a::text").get("").strip()
        posts = response.css(".postbody, .post-content, div[id^='post_message']")
        
        if not posts:
            return

        first_post = posts[0].css("::text").getall()
        body = " ".join(first_post).strip()

        # Look for solved/solution in replies
        solution_text = "Unresolved"
        for post in posts[1:]:
            post_text = " ".join(post.css("::text").getall()).strip()
            sol = extract_solution(post_text)
            if sol != "Unresolved":
                solution_text = sol
                break
        
        if len(body) < 30:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body),
            "hardware_env": detect_hw_env(body),
            "problem": (title + " " + body[:1000]).strip(),
            "raw_logs": "",
            "solution": solution_text,
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }

        # Paginate thread replies
        next_page = response.css("a[rel='next']::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse_thread)


class ArchForumsSpider(scrapy.Spider):
    name = "arch_forums"
    allowed_domains = ["bbs.archlinux.org"]
    start_urls = [
        "https://bbs.archlinux.org/index.php",
    ]
    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DEPTH_LIMIT": 6,
        "CLOSESPIDER_ITEMCOUNT": 60000,
        "FEEDS": {"arch_forums_output.jsonl": {"format": "jsonlines", "overwrite": True}},
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if "viewforum.php" in full:
                yield scrapy.Request(full, callback=self.parse_forum)

    def parse_forum(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if "viewtopic.php" in full:
                yield scrapy.Request(full, callback=self.parse_topic)
        
        next_page = response.css("a[rel=next]::attr(href), .pagination a:last-child::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse_forum)

    def parse_topic(self, response):
        title = response.css("h1::text, h2::text").get("").strip()
        posts = response.css(".post_body, .entry-content, div.postprofile + div")
        
        if not posts:
            body_text = " ".join(response.css("div.post::text").getall()).strip()
        else:
            body_text = " ".join(posts[0].css("::text").getall()).strip()

        solution_text = "Unresolved"
        for post in posts[1:]:
            post_text = " ".join(post.css("::text").getall()).strip()
            sol = extract_solution(post_text)
            if sol != "Unresolved":
                solution_text = sol
                break

        if len(body_text) < 30:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body_text),
            "hardware_env": detect_hw_env(body_text),
            "problem": (title + " " + body_text[:1000]).strip(),
            "raw_logs": "",
            "solution": solution_text,
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }

        next_page = response.css("a[rel=next]::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse_topic)


class UbuntuForumsSpider(scrapy.Spider):
    name = "ubuntu_forums"
    allowed_domains = ["ubuntuforums.org"]
    start_urls = [
        "https://ubuntuforums.org/forumdisplay.php?f=48",   # General Help
        "https://ubuntuforums.org/forumdisplay.php?f=333",  # Installation & Upgrades
        "https://ubuntuforums.org/forumdisplay.php?f=338",  # Hardware
        "https://ubuntuforums.org/forumdisplay.php?f=336",  # Networking
        "https://ubuntuforums.org/forumdisplay.php?f=100",  # Desktop Environments
        "https://ubuntuforums.org/forumdisplay.php?f=339",  # Security
        "https://ubuntuforums.org/forumdisplay.php?f=335",  # Software
        "https://ubuntuforums.org/forumdisplay.php?f=350",  # Server Platforms
        "https://ubuntuforums.org/forumdisplay.php?f=27",   # Kernel
    ]
    custom_settings = {
        "DOWNLOAD_DELAY": 2.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DEPTH_LIMIT": 5,
        "CLOSESPIDER_ITEMCOUNT": 60000,
        "FEEDS": {"ubuntu_forums_output.jsonl": {"format": "jsonlines", "overwrite": True}},
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        for link in response.css("a::attr(href)").getall():
            full = urljoin(response.url, link)
            if "showthread.php" in full:
                yield scrapy.Request(full, callback=self.parse_thread)
        
        next_page = response.css("a[rel=next]::attr(href), .pagination a:last-child::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse)

    def parse_thread(self, response):
        title = response.css("h1::text, .threadtitle::text").get("").strip()
        posts = response.css(".postcontent, .postbody")
        
        if not posts:
            return
        
        body = " ".join(posts[0].css("::text").getall()).strip()
        solution_text = "Unresolved"
        for post in posts[1:]:
            pt = " ".join(post.css("::text").getall()).strip()
            sol = extract_solution(pt)
            if sol != "Unresolved":
                solution_text = sol
                break

        if len(body) < 30:
            return

        yield {
            "doc_id": make_doc_id(response.url),
            "domain": detect_domain(response.url, body),
            "hardware_env": detect_hw_env(body),
            "problem": (title + " " + body[:1000]).strip(),
            "raw_logs": "",
            "solution": solution_text,
            "source_file": "heuristically_parsed",
            "original_link": response.url,
        }

        next_page = response.css("a[rel=next]::attr(href)").get()
        if next_page:
            yield scrapy.Request(urljoin(response.url, next_page), callback=self.parse_thread)
