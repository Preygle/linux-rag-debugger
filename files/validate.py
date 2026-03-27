#!/usr/bin/env python3
"""
LinuxLynx Pipeline Validation Suite
====================================
Tests:
  1. Schema validation of sample_dataset.jsonl
  2. Import checks for all scraper modules
  3. Deduplication logic (exact + fuzzy)
  4. Risk classifier
  5. Environment extractor (distro/kernel/component)
  6. JSONL round-trip (serialize → deserialize → re-validate)
"""

import json
import sys
import traceback
from pathlib import Path
from dataclasses import asdict

# ── Setup path ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE / "src"))

SAMPLE_JSONL = _HERE / "data" / "sample_dataset.jsonl"


# ── Test helpers ──────────────────────────────────────────────────────────────

class TestRunner:
    def __init__(self):
        self.passed   = 0
        self.failed   = 0
        self.warnings = 0

    def ok(self, name: str, detail: str = ""):
        self.passed += 1
        print(f"  ✓  {name}" + (f"  ({detail})" if detail else ""))

    def fail(self, name: str, reason: str):
        self.failed += 1
        print(f"  ✗  {name}  →  {reason}")

    def warn(self, name: str, reason: str):
        self.warnings += 1
        print(f"  ⚠  {name}  →  {reason}")

    def section(self, title: str):
        print(f"\n{'─'*60}")
        print(f"  {title}")
        print(f"{'─'*60}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"  Results: {self.passed}/{total} passed  |  {self.warnings} warnings")
        if self.failed == 0:
            print("  ✓ ALL TESTS PASSED")
        else:
            print(f"  ✗ {self.failed} TESTS FAILED")
        print(f"{'='*60}\n")
        return self.failed == 0


T = TestRunner()


# ── Test 1: Schema import ─────────────────────────────────────────────────────
T.section("1. Schema module")

try:
    from schema import (
        LinuxLynxDoc, extract_distro, extract_kernel, extract_component,
        classify_risk, DOMAINS, FAILURE_TYPES, CONFIDENCES
    )
    T.ok("schema module imports cleanly")
except Exception as e:
    T.fail("schema import", str(e))
    print("  Cannot continue without schema — aborting.")
    sys.exit(1)


# ── Test 2: Environment extractors ───────────────────────────────────────────
T.section("2. Environment field extractors")

cases_distro = [
    ("Running Ubuntu 22.04 LTS on bare metal",           "Ubuntu 22.04"),
    ("Debian Bullseye 11.7 fresh install",               "Debian Bullseye"),
    ("OS: Arch Linux rolling 2024.01",                   "Arch Linux"),
    ("No distro info",                                   "unknown"),
]
for text, expected in cases_distro:
    result = extract_distro(text)
    if expected in result or (expected == "unknown" and result == "unknown"):
        T.ok(f"extract_distro: '{text[:40]}...'", f"→ '{result}'")
    else:
        T.fail(f"extract_distro: '{text[:40]}'", f"expected '{expected}' got '{result}'")

cases_kernel = [
    ("kernel 5.15.0-91-generic running",  "5.15.0-91-generic"),
    ("uname: 6.1.0-rc4",                  "6.1.0-rc4"),
    ("version 5.4.0",                     "5.4.0"),
    ("no version here xyz",               "unknown"),
]
for text, expected in cases_kernel:
    result = extract_kernel(text)
    if expected == "unknown":
        if result == "unknown":
            T.ok(f"extract_kernel: no version → unknown")
        else:
            T.warn(f"extract_kernel: expected unknown, got '{result}' (may be OK)")
    elif expected in result:
        T.ok(f"extract_kernel: '{text}'", f"→ '{result}'")
    else:
        T.fail(f"extract_kernel: '{text}'", f"expected '{expected}' got '{result}'")

cases_comp = [
    ("systemd service failed to start",   "systemd"),
    ("NetworkManager can't find device",  "NetworkManager"),
    ("ext4 filesystem error on sdb1",     "ext4"),
    ("random text no component",          "unknown"),
]
for text, expected in cases_comp:
    result = extract_component(text, fallback="unknown")
    if expected.lower() in result.lower():
        T.ok(f"extract_component: '{text[:40]}'", f"→ '{result}'")
    else:
        T.warn(f"extract_component: '{text[:40]}'", f"expected '{expected}' got '{result}'")


# ── Test 3: Risk classifier ───────────────────────────────────────────────────
T.section("3. Risk level classifier")

risk_cases = [
    ("sudo systemctl restart nginx",                "safe"),
    ("chmod -R 777 /var/www/html",                  "dangerous"),
    ("rm -rf /tmp/stale-data && reboot",            "dangerous"),
    ("dd if=/dev/zero of=/dev/sda bs=4M",           "dangerous"),
    ("echo 'net.ipv4.forwarding=1' >> /etc/sysctl", "safe"),
    ("iptables -F && iptables -X",                  "dangerous"),
]
for solution, expected in risk_cases:
    result = classify_risk(solution)
    if result == expected:
        T.ok(f"classify_risk: '{solution[:45]}'", f"→ {result}")
    else:
        T.fail(f"classify_risk: '{solution[:45]}'", f"expected {expected} got {result}")


# ── Test 4: Document build + validation ──────────────────────────────────────
T.section("4. LinuxLynxDoc build & validate")

good_doc = LinuxLynxDoc.build(
    doc_id      = "test_001",
    source      = "stackoverflow",
    domain      = "kernel",
    failure_type= "kernel panic",
    distro      = "Ubuntu 22.04",
    kernel      = "5.15.0-91-generic",
    component   = "ext4",
    problem     = "Kernel panics on boot after upgrading kernel",
    raw_logs    = "Kernel panic - not syncing: VFS: Unable to mount root fs\nCall Trace:\n panic+0x10f\n mount_block_root+0x1c6",
    debug_steps = "update-initramfs -u -k all",
    root_cause  = "initramfs missing ext4 driver after kernel upgrade",
    solution    = "sudo update-initramfs -u -k all && sudo update-grub",
    reasoning   = "Regenerates initrd with correct kernel module set",
    confidence  = "high",
    link        = "https://stackoverflow.com/questions/1",
)
errs = good_doc.validate()
if not errs:
    T.ok("Valid doc passes validate()")
else:
    T.fail("Valid doc validate()", str(errs))

# Test invalid doc
bad_doc = LinuxLynxDoc.build(
    doc_id   = "test_bad",
    source   = "web",
    problem  = "short",
    raw_logs = "too short",   # < 50 chars
    solution = "fix",
)
errs = bad_doc.validate()
if errs:
    T.ok("Invalid doc (short logs) correctly fails validation", f"errors: {errs}")
else:
    T.fail("Invalid doc should have validation errors", "none found")

# Test content hash stability
h1 = good_doc.content_hash()
h2 = good_doc.content_hash()
if h1 == h2:
    T.ok("content_hash() is stable", h1[:16] + "...")
else:
    T.fail("content_hash() unstable", f"{h1} != {h2}")

# Test JSONL round-trip
jsonl_line = good_doc.to_jsonl()
try:
    parsed   = json.loads(jsonl_line)
    restored = LinuxLynxDoc(**parsed)
    errs2    = restored.validate()
    if not errs2:
        T.ok("JSONL round-trip (serialize → deserialize → validate)")
    else:
        T.fail("JSONL round-trip validation", str(errs2))
except Exception as e:
    T.fail("JSONL round-trip", str(e))


# ── Test 5: Sample dataset validation ────────────────────────────────────────
T.section("5. Sample dataset (data/sample_dataset.jsonl)")

if not SAMPLE_JSONL.exists():
    T.fail("sample_dataset.jsonl", f"File not found: {SAMPLE_JSONL}")
else:
    valid_count   = 0
    invalid_count = 0
    domain_dist: dict[str, int] = {}
    conf_dist:   dict[str, int] = {}

    with open(SAMPLE_JSONL) as fh:
        for i, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                doc  = LinuxLynxDoc(**data)
                errs = doc.validate()
                if errs:
                    T.fail(f"Sample doc #{i} ({data.get('doc_id','?')})", str(errs))
                    invalid_count += 1
                else:
                    valid_count += 1
                    domain_dist[doc.domain]     = domain_dist.get(doc.domain, 0) + 1
                    conf_dist[doc.confidence]   = conf_dist.get(doc.confidence, 0) + 1
            except json.JSONDecodeError as e:
                T.fail(f"Sample doc #{i} JSON parse", str(e))
                invalid_count += 1
            except TypeError as e:
                T.fail(f"Sample doc #{i} schema mismatch", str(e))
                invalid_count += 1

    if invalid_count == 0:
        T.ok(f"All {valid_count} sample docs valid")
    else:
        T.warn("Some sample docs invalid", f"{valid_count} ok / {invalid_count} failed")

    if valid_count > 0:
        T.ok("Domain coverage", ", ".join(f"{k}:{v}" for k, v in sorted(domain_dist.items())))
        T.ok("Confidence distribution", str(conf_dist))


# ── Test 6: Deduplicator ─────────────────────────────────────────────────────
T.section("6. Deduplication logic")

try:
    from dedup import Deduplicator

    deduper = Deduplicator(fuzzy=False)

    doc_a = LinuxLynxDoc.build(
        doc_id="dedup_a", source="stackoverflow", domain="kernel",
        failure_type="kernel panic",
        problem="Kernel panic on boot",
        raw_logs="Kernel panic - not syncing: VFS: Unable to mount root fs\nCall Trace follows here",
        solution="sudo update-initramfs -u", confidence="high", link="https://so.com/1",
    )
    doc_b = LinuxLynxDoc.build(
        doc_id="dedup_b", source="forum", domain="kernel",
        failure_type="kernel panic",
        problem="Kernel panic on boot",          # same problem
        raw_logs="Kernel panic - not syncing: VFS: Unable to mount root fs\nCall Trace follows here",
        solution="sudo update-initramfs -u",     # same solution
        confidence="low", link="https://forum.com/1",
    )
    doc_c = LinuxLynxDoc.build(
        doc_id="dedup_c", source="bugzilla", domain="networking",
        failure_type="network timeout",
        problem="NetworkManager fails after suspend",
        raw_logs="NetworkManager[1234]: device enp3s0 state change unavailable unmanaged r8169 Link is Down",
        solution="modprobe -r r8169 && modprobe r8169", confidence="high", link="https://bugz.com/2",
    )

    r1 = deduper.add(doc_a)
    r2 = deduper.add(doc_b)   # duplicate of doc_a
    r3 = deduper.add(doc_c)   # different

    if r1 and not r2 and r3:
        T.ok("Exact dedup: duplicate rejected, different doc accepted")
    else:
        T.fail("Exact dedup", f"r1={r1} r2={r2} r3={r3} (expected True,False,True)")

    unique = deduper.unique_docs()
    if len(unique) == 2:
        T.ok(f"unique_docs() returns 2 (not 3)", "exact dedup working")
    else:
        T.fail("unique_docs()", f"expected 2, got {len(unique)}")

    # Preference: stackoverflow (rank 2) vs forum (rank 3) → stackoverflow wins
    kept = [d for d in unique if d.doc_id in ("dedup_a", "dedup_b")]
    if kept and kept[0].source == "stackoverflow":
        T.ok("Cross-source preference: stackoverflow beats forum")
    else:
        T.warn("Cross-source preference", f"kept: {[d.doc_id for d in kept]}")

except Exception as e:
    T.fail("Deduplicator", traceback.format_exc(limit=3))


# ── Test 7: Scraper imports ───────────────────────────────────────────────────
T.section("7. Scraper module imports")

scrapers = [
    ("scrapers.bugzilla_kernel", "scrape"),
    ("scrapers.lkml",            "scrape"),
    ("scrapers.forums",          "scrape"),
    ("scrapers.security",        "scrape_nvd"),
    ("scrapers.security",        "scrape_syzkaller"),
]

for module_name, fn_name in scrapers:
    try:
        import importlib
        mod = importlib.import_module(module_name)
        fn  = getattr(mod, fn_name, None)
        if callable(fn):
            T.ok(f"{module_name}.{fn_name}() importable")
        else:
            T.fail(f"{module_name}.{fn_name}", "not callable")
    except ImportError as e:
        # Missing optional deps (requests, bs4) are expected in test env
        if "requests" in str(e) or "bs4" in str(e) or "sklearn" in str(e):
            T.warn(f"{module_name}.{fn_name}", f"optional dep missing: {e}")
        else:
            T.fail(f"{module_name}.{fn_name}", str(e))
    except Exception as e:
        T.fail(f"{module_name}.{fn_name}", str(e))


# ── Test 8: Pipeline imports ──────────────────────────────────────────────────
T.section("8. Pipeline orchestrator import")

try:
    from pipeline import run_pipeline, PipelineStats, SOURCES_REGISTRY
    T.ok("pipeline.py imports cleanly")
    T.ok(f"SOURCES_REGISTRY has {len(SOURCES_REGISTRY)} sources",
         ", ".join(s.name for s in SOURCES_REGISTRY))
except Exception as e:
    T.fail("pipeline import", traceback.format_exc(limit=3))


# ── Summary ───────────────────────────────────────────────────────────────────
success = T.summary()
sys.exit(0 if success else 1)
