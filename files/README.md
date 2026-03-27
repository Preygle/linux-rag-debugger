# LinuxLynx Data Collection Pipeline

A production-ready scraping + dataset pipeline that collects real-world Linux failure scenarios from multiple high-signal sources, deduplicates them, and outputs a JSONL dataset ready for RAG ingestion.

---

## Project Structure

```
linuxlynx/
├── src/
│   ├── schema.py                  # JSONL schema, validation, field extractors
│   ├── dedup.py                   # Exact hash + TF-IDF fuzzy deduplication
│   ├── pipeline.py                # Orchestrator — runs all scrapers end-to-end
│   └── scrapers/
│       ├── bugzilla_kernel.py     # Kernel Bugzilla (RESOLVED FIXED bugs)
│       ├── lkml.py                # LKML / lore.kernel.org mailing lists
│       ├── forums.py              # LinuxQuestions, Arch BBS, Ubuntu Forums [SOLVED]
│       ├── security.py            # NVD CVE database + Syzkaller crash reports
│       ├── stack_exchange.py      # (existing) Stack Overflow / Server Fault / Ask Ubuntu
│       ├── github.py              # (existing) GitHub Issues
│       └── web_crawler.py         # (existing) Arch Wiki
├── data/
│   └── sample_dataset.jsonl       # 10 hand-crafted validation entries
└── validate.py                    # Full test suite (36 tests)
```

---

## Quick Start

### Install dependencies
```bash
pip install requests beautifulsoup4 scikit-learn
```

### Run validation suite
```bash
python validate.py
# Expected: 36/36 passed
```

### Run the full pipeline
```bash
# All sources, default limits (~1100 docs max)
python src/pipeline.py --output dataset.jsonl

# Specific sources only
python src/pipeline.py --sources bugzilla nvd syzkaller --output security.jsonl

# With NVD API key (higher rate limits: 50 req/30s vs 5 req/30s)
python src/pipeline.py --nvd-api-key YOUR_KEY --output dataset.jsonl

# Limit per source (useful for testing)
python src/pipeline.py --max 10 --output test_sample.jsonl
```

### Run individual scrapers
```bash
# Kernel Bugzilla
python src/scrapers/bugzilla_kernel.py bugzilla_out.jsonl

# LKML mailing list
python src/scrapers/lkml.py lkml_out.jsonl

# Forums (LinuxQuestions + Arch BBS + Ubuntu Forums)
python src/scrapers/forums.py forums_out.jsonl

# NVD CVEs
python src/scrapers/security.py nvd nvd_out.jsonl

# Syzkaller crashes
python src/scrapers/security.py syzkaller syzkaller_out.jsonl
```

### Dedup an existing file
```bash
python src/dedup.py input.jsonl output.jsonl
```

---

## JSONL Schema

Every document is a single-line JSON object:

```json
{
  "doc_id":       "<source>_<id>",
  "source":       "stackoverflow | askubuntu | serverfault | bugzilla | lkml | forum | web | github_<repo>",
  "domain":       "networking | kernel | systemd | filesystem | security | hardware | package | boot | memory | process | storage | container | virtualization | other",
  "failure_type": "segfault | permission | dependency | kernel panic | network timeout | disk corruption | config error | other",
  "environment":  { "distro": "Ubuntu 22.04", "kernel": "5.15.0-91", "component": "ext4" },
  "problem":      "1-2 sentence description",
  "raw_logs":     "exact error messages, stack traces, dmesg output",
  "debug_steps":  "commands used to diagnose before fix was found",
  "root_cause":   "underlying technical reason (not the symptom)",
  "solution":     "accepted fix — commands, config changes, patch",
  "reasoning":    "why this fix addresses the root cause",
  "risk_level":   "safe | caution | dangerous",
  "version_scope":"affected / fixed-in versions",
  "confidence":   "high | medium | low",
  "link":         "original URL"
}
```

### Confidence scoring
| Score | Criteria |
|---|---|
| `high` | Accepted Stack Exchange answer, RESOLVED FIXED bugzilla, NVD CVE, Syzkaller fixed |
| `medium` | `[SOLVED]` forum thread, mailing list with confirmed patch |
| `low` | Unconfirmed reply, speculative thread |

---

## New Scrapers — Design Notes

### `bugzilla_kernel.py`
- Uses Kernel Bugzilla REST API (`/rest/bug`)
- Filters: `status=RESOLVED|VERIFIED`, `resolution=FIXED`
- Extracts `raw_logs` from first comment (reporter's description), code blocks preferred
- Finds fix comment by looking for commit hashes or patch URLs
- Auto-maps `component` field to `domain` enum
- Rate limit: 1.5s between requests

### `lkml.py`
- Searches `lore.kernel.org` full-text for kernel failure keywords across 6 high-signal lists
- Thread validity filters (all must be true):
  1. Contains a diff/patch or commit hash
  2. Last 3 messages contain resolution language ("fixed", "applied", "works now")
  3. Subject is NOT `[RFC]` or `[PATCH WIP]`
- Extracts kernel-format log lines (lines starting with `[timestamp]`, `BUG:`, `Call Trace:`, etc.)
- Rate limit: 2s between requests

### `forums.py`
- Covers LinuxQuestions.org, Arch BBS, Ubuntu Forums
- Only fetches threads with `[SOLVED]` in title (search + HTML filter)
- Shared phpBB-style parser extracts first post (problem) and solution post
- Code block content (`<pre>`, `<code>`) promoted to `raw_logs`
- 15 failure-type keywords drive searches; 2 search result pages per keyword per site

### `security.py` (NVD + Syzkaller)
**NVD:**
- REST API v2 (`/rest/json/cves/2.0`) — no API key needed, but key recommended (10x rate limit)
- Filters: CVSS >= 6.0, keyword in description matches Linux components
- Maps CWE IDs to `failure_type` enum
- Extracts affected versions from CPE strings

**Syzkaller:**
- Fetches `/upstream/fixed` page — only confirmed-fixed crashes
- Extracts KASAN/KASAN/panic output from `<pre>` blocks as `raw_logs`
- Links to fix commit on git.kernel.org
- `confidence=high` if fix commit found, `medium` otherwise

---

## Deduplication Pipeline (`dedup.py`)

Applied in order:
1. **Exact hash** — SHA-256 of normalized `(problem + raw_logs + solution)`; removes true duplicates
2. **Fuzzy similarity** — TF-IDF cosine similarity > 0.90; keeps the higher-confidence / better-source entry
3. **Cross-source preference** — bugzilla > lkml/github > stackoverflow > forum > web

Fuzzy dedup requires `scikit-learn`. Disable with `--no-fuzzy-dedup` for speed.

---

## Extending the Pipeline

### Add a new source
1. Create `src/scrapers/mysource.py` with a `scrape(**kwargs) -> Generator[LinuxLynxDoc]` function
2. Add an entry to `SOURCES_REGISTRY` in `src/pipeline.py`
3. Add the load case in `_load_scraper()` in `src/pipeline.py`
4. Add an import test case to `validate.py`

### Add new Stack Exchange tags
Edit `src/scrapers/stack_exchange.py` and add to the tags list:
```python
TAGS = [
    "linux", "bash",
    # New high-signal tags:
    "systemd", "kernel", "ext4", "btrfs", "grub", "docker",
    "selinux", "apparmor", "kvm", "qemu", "lvm", "luks",
    "nfs", "openssh", "rsyslog", "journald", "udev", "dkms",
]
```

### Add new GitHub repos
Edit `src/scrapers/github.py` repos list:
```python
REPOS = [
    "systemd/systemd",           # already scraped
    "torvalds/linux",
    "NetworkManager/NetworkManager",
    "util-linux/util-linux",
    "openssh/openssh-portable",
    "containers/podman",
    "moby/moby",
    "lvm2/lvm2",
    "dracut-ng/dracut-ng",
]
```

---

## Sample Dataset Coverage

The `data/sample_dataset.jsonl` file contains 10 validated entries spanning:

| Domain | Scenario |
|---|---|
| kernel | VFS root mount panic after kernel upgrade (ext4 initramfs) |
| networking | NetworkManager fails after suspend (r8169 NIC driver) |
| systemd | Type=notify service timeout (sd_notify not implemented) |
| memory | NULL pointer deref in __alloc_pages (CMA failure) |
| filesystem | btrfs overlapping extents after balance (read-only remount) |
| boot | GRUB rescue after Windows dual-boot overwrites ESP |
| security | CVE-2023-32233 nftables use-after-free privilege escalation |
| kernel | Syzkaller KASAN slab-out-of-bounds in Bluetooth HCI |
| container | Docker port 80 bind permission denied (CAP_NET_BIND_SERVICE) |
| package | apt unmet dependencies after PPA conflict (python3 versions) |
