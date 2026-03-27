# LinuxLynx Scraper

Collects Linux kernel bug reports, CVEs, mailing list threads, and forum posts
into a unified JSONL dataset for RAG / fine-tuning.

## Project Structure

```
linuxlynx_scraper/
├── main.py                  ← entry point (run this)
├── requirements.txt
├── .env.example             ← copy to .env and add your NVD key
├── data/                    ← output .jsonl files land here
└── src/
    ├── schema.py            ← LinuxLynxDoc dataclass + validation
    ├── dedup.py             ← cross-run deduplication (MD5 hash store)
    ├── ingest.py            ← programmatic API used by other tools
    └── scrapers/
        ├── bugzilla_kernel.py   ← https://bugzilla.kernel.org
        ├── lkml.py              ← https://lore.kernel.org (LKML + sublists)
        ├── forums.py            ← LinuxQuestions, Arch BBS, Ubuntu Forums
        └── security.py          ← NVD CVE database + Syzbot
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) Set NVD API key for faster CVE scraping
cp .env.example .env
# edit .env and add your key from https://nvd.nist.gov/developers/request-an-api-key

# 3. Run a scraper
python main.py --source bugzilla  --limit 50
python main.py --source lkml      --limit 100
python main.py --source forums    --limit 50
python main.py --source nvd       --limit 200
python main.py --source syzkaller --limit 100

# 4. Run ALL sources (outputs to data/<source>.jsonl)
python main.py --source all --limit 50

# 5. Verbose logging
python main.py --source bugzilla --limit 10 --verbose
```

## Output Format

Each line in the `.jsonl` file is a JSON object conforming to `LinuxLynxDoc`:

```json
{
  "doc_id": "bugzilla_12345",
  "source": "bugzilla",
  "domain": "networking",
  "failure_type": "kernel panic",
  "environment": {"distro": "Ubuntu 22.04", "kernel": "5.15.0-91-generic", "component": "NetworkManager"},
  "problem": "One-line description of the issue",
  "raw_logs": "[ 123.456] BUG: unable to handle kernel NULL pointer...",
  "debug_steps": "Maintainer back-and-forth / bisect steps",
  "root_cause": "Race condition in net/core/dev.c introduced in 6.1-rc1",
  "solution": "Commit abc1234\n\nApply patch from lore.kernel.org/...",
  "reasoning": "The fix serialises the relevant code path with a spinlock",
  "risk_level": "safe",
  "version_scope": "5.15, 6.1",
  "confidence": "high",
  "link": "https://bugzilla.kernel.org/show_bug.cgi?id=12345"
}
```

## Sources

| Source      | URL                                  | Confidence | Notes                          |
|-------------|--------------------------------------|------------|--------------------------------|
| `bugzilla`  | bugzilla.kernel.org                  | high       | RESOLVED FIXED only            |
| `lkml`      | lore.kernel.org                      | medium     | threads with patch + fix reply |
| `forums`    | linuxquestions, archbbs, ubuntuforums| medium     | [SOLVED] threads only          |
| `nvd`       | services.nvd.nist.gov                | high       | CVSS ≥ 6.0, Linux-related only |
| `syzkaller` | syzkaller.appspot.com                | high       | fixed fuzzer crashes only      |

## Notes

- **Rate limiting** is built in — each scraper has a `DELAY_SECONDS` constant.
- **Deduplication** is persistent across runs via `data/seen_hashes.json`.
- **NVD without API key**: 5 req/30s. With key: 50 req/30s.
- Forums may return fewer results if a site blocks bots — this is normal.
