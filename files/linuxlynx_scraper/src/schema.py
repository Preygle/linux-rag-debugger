"""
LinuxLynx JSONL schema definition and validation utilities.
All scrapers must produce documents conforming to this schema.
"""

from __future__ import annotations
import hashlib
import json
import re
from dataclasses import dataclass, asdict, field
from typing import Literal, Optional


# ── Enums ────────────────────────────────────────────────────────────────────

DOMAINS = {
    "networking", "kernel", "systemd", "filesystem", "security",
    "hardware", "package", "boot", "memory", "process",
    "storage", "container", "virtualization", "other",
}

FAILURE_TYPES = {
    "segfault", "permission", "dependency", "kernel panic",
    "network timeout", "disk corruption", "config error", "other",
}

SOURCES = {
    "stackoverflow", "askubuntu", "serverfault",
    "bugzilla", "lkml", "forum", "web",
    # github_<repo> is also valid (checked by prefix)
}

RISK_LEVELS   = {"safe", "caution", "dangerous"}
CONFIDENCES   = {"high", "medium", "low"}


# ── Distro / Kernel extraction helpers ───────────────────────────────────────

_DISTRO_RE = re.compile(
    r"(Ubuntu\s+\d+\.\d+(?:\.\d+)?(?:\s+\w+)?|"
    r"Debian\s+\w+|"
    r"Arch\s+Linux|"
    r"CentOS\s+\d+(?:\.\d+)?|"
    r"Fedora\s+\d+|"
    r"openSUSE\s+\w+|"
    r"RHEL\s+\d+(?:\.\d+)?|"
    r"Linux\s+Mint\s+\d+(?:\.\d+)?|"
    r"Manjaro\s*\w*|"
    r"Rocky\s+Linux\s+\d+(?:\.\d+)?|"
    r"AlmaLinux\s+\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

_KERNEL_RE = re.compile(
    r"\b(\d+\.\d+(?:\.\d+)?(?:-[\w.]+)?)\b"
)

_KERNEL_FULL_RE = re.compile(
    r"\b(\d+\.\d+\.\d+-\d+[-\w]*(?:generic|amd64|x86_64|lts)?)\b"
)


def extract_distro(text: str) -> str:
    m = _DISTRO_RE.search(text)
    return m.group(0).strip() if m else "unknown"


def extract_kernel(text: str) -> str:
    # Prefer full strings like 5.15.0-91-generic
    m = _KERNEL_FULL_RE.search(text)
    if m:
        return m.group(1)
    # Fall back to short version like 6.1
    for m in _KERNEL_RE.finditer(text):
        v = m.group(1)
        parts = v.split(".")
        if len(parts) >= 2 and all(p.isdigit() for p in parts[:2]):
            return v
    return "unknown"


def extract_component(text: str, fallback: str = "unknown") -> str:
    """
    Try to pull the primary failing component from log text.
    Looks for known service/tool names mentioned near error keywords.
    """
    _COMPONENTS = [
        "systemd", "NetworkManager", "grub", "dracut", "mkinitcpio",
        "ext4", "btrfs", "xfs", "zfs", "lvm", "dm-crypt", "luks",
        "docker", "podman", "containerd", "kvm", "qemu",
        "openssh", "sshd", "nginx", "apache2", "httpd",
        "journald", "rsyslog", "udev", "dbus", "polkit",
        "nfs", "samba", "iptables", "nftables", "firewalld",
        "kernel", "initramfs", "cron", "auditd", "selinux", "apparmor",
    ]
    lower = text.lower()
    for comp in _COMPONENTS:
        if comp.lower() in lower:
            return comp
    return fallback


# ── Risk classification ───────────────────────────────────────────────────────

_DANGEROUS_PATTERNS = re.compile(
    r"rm\s+-rf|chmod\s+-R\s+777|dd\s+if=|mkfs\.|format\s+/|"
    r"wipefs|shred\s+-|fdisk|parted|:\s*>\s*/dev/s[db]|"
    r"mv\s+/\s+|iptables\s+-F|nft\s+flush",
    re.IGNORECASE,
)

_CAUTION_PATTERNS = re.compile(
    r"chmod\s+|chown\s+|systemctl\s+disable|systemctl\s+mask|"
    r"sysctl\s+-w|modprobe\s+-r|rmmod\s+|iptables\s+-[ADI]|"
    r"useradd|userdel|passwd\s+|visudo|sudoers",
    re.IGNORECASE,
)


def classify_risk(solution_text: str) -> str:
    if _DANGEROUS_PATTERNS.search(solution_text):
        return "dangerous"
    if _CAUTION_PATTERNS.search(solution_text):
        return "caution"
    return "safe"


# ── Document dataclass ────────────────────────────────────────────────────────

@dataclass
class LinuxLynxDoc:
    doc_id: str
    source: str
    domain: str
    failure_type: str
    environment: dict          # {distro, kernel, component}
    problem: str
    raw_logs: str
    debug_steps: str
    root_cause: str
    solution: str
    reasoning: str
    risk_level: str
    version_scope: str
    confidence: str
    link: str

    # ── Convenience constructors ──────────────────────────────────────────────

    @classmethod
    def build(
        cls,
        *,
        doc_id: str,
        source: str,
        domain: str = "other",
        failure_type: str = "other",
        distro: str = "unknown",
        kernel: str = "unknown",
        component: str = "unknown",
        problem: str,
        raw_logs: str,
        debug_steps: str = "",
        root_cause: str = "unknown",
        solution: str,
        reasoning: str = "",
        risk_level: str | None = None,
        version_scope: str = "unknown",
        confidence: str = "medium",
        link: str = "",
    ) -> "LinuxLynxDoc":
        return cls(
            doc_id=doc_id,
            source=source,
            domain=domain if domain in DOMAINS else "other",
            failure_type=failure_type if failure_type in FAILURE_TYPES else "other",
            environment={"distro": distro, "kernel": kernel, "component": component},
            problem=problem,
            raw_logs=raw_logs,
            debug_steps=debug_steps,
            root_cause=root_cause,
            solution=solution,
            reasoning=reasoning,
            risk_level=risk_level if risk_level in RISK_LEVELS
                        else classify_risk(solution),
            version_scope=version_scope,
            confidence=confidence if confidence in CONFIDENCES else "medium",
            link=link,
        )

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_jsonl(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    def content_hash(self) -> str:
        key = "\n".join([
            self.problem.lower().strip(),
            self.raw_logs.lower().strip(),
            self.solution.lower().strip(),
        ])
        return hashlib.sha256(key.encode()).hexdigest()

    # ── Validation ────────────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Return list of validation errors; empty list = valid."""
        errors: list[str] = []
        if len(self.raw_logs) < 50:
            errors.append(f"raw_logs too short ({len(self.raw_logs)} chars)")
        if not self.problem:
            errors.append("problem is empty")
        if not self.solution:
            errors.append("solution is empty")
        if self.domain not in DOMAINS:
            errors.append(f"invalid domain: {self.domain}")
        if self.failure_type not in FAILURE_TYPES:
            errors.append(f"invalid failure_type: {self.failure_type}")
        if self.confidence not in CONFIDENCES:
            errors.append(f"invalid confidence: {self.confidence}")
        return errors
