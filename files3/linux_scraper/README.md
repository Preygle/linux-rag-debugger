# Linux RAG Scraper Suite

A dual-engine scraper system (Scrapy + Crawl4AI) to collect Linux issue data for RAG vector embeddings.

## Output Format

```jsonl
{"doc_id": "web_abc123", "domain": "kernel/boot", "hardware_env": "arch, systemd, x86_64",
 "problem": "...", "raw_logs": "...", "solution": "Unresolved or extracted fix",
 "source_file": "heuristically_parsed", "original_link": "https://..."}
```

---

## Sources Covered

| Site | Type | Est. Docs |
|------|------|-----------|
| lkml.org | Kernel mailing list | ~500K+ |
| lore.kernel.org | Kernel mailing (13 lists) | ~800K+ |
| lists.debian.org | Debian lists (10 lists) | ~400K+ |
| lists.ubuntu.com | Ubuntu lists (7 lists) | ~300K+ |
| linuxquestions.org | Q&A forum | ~500K+ |
| bbs.archlinux.org | Arch forum | ~200K+ |
| ubuntuforums.org | Ubuntu forum | ~300K+ |
| wiki.archlinux.org | Wiki articles | ~10K |
| docs.kernel.org | Kernel docs | ~20K |
| man7.org | Man pages | ~5K |

---

## Setup

### 1. Scrapy Engine

```bash
cd linux_scraper/scrapy_scraper
pip install -r requirements.txt
```

### 2. Crawl4AI Engine

```bash
cd linux_scraper/crawl4ai_scraper
pip install -r requirements.txt
pip install crawl4ai
crawl4ai-setup          # installs Playwright browsers
playwright install chromium
```

---

## ═══ SCRAPY: Run Commands ═══

Run from inside `linux_scraper/scrapy_scraper/` directory.

### Run ALL spiders (sequentially)

```bash
for spider in lkml lore_kernel debian_lists ubuntu_lists linux_questions arch_forums ubuntu_forums; do
    scrapy crawl $spider
done
```

### Individual spiders — MAX LIMIT commands

#### LKML (Linux Kernel Mailing List)
```bash
# Max: unlimited (50,000 default — remove to crawl everything)
scrapy crawl lkml -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=1.0 -s CONCURRENT_REQUESTS_PER_DOMAIN=6 -o lkml_output.jsonl

# Conservative (50K items)
scrapy crawl lkml -s CLOSESPIDER_ITEMCOUNT=50000 -o lkml_output.jsonl
```

#### Lore Kernel (13 subsystem lists)
```bash
# Max unlimited
scrapy crawl lore_kernel -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=1.0 -s CONCURRENT_REQUESTS_PER_DOMAIN=6 -o lore_kernel_output.jsonl

# 100K cap
scrapy crawl lore_kernel -s CLOSESPIDER_ITEMCOUNT=100000 -o lore_kernel_output.jsonl
```

#### Debian Mailing Lists (10 lists)
```bash
scrapy crawl debian_lists -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=1.5 -s CONCURRENT_REQUESTS_PER_DOMAIN=4 -o debian_lists_output.jsonl
```

#### Ubuntu Mailing Lists
```bash
scrapy crawl ubuntu_lists -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=1.5 -o ubuntu_lists_output.jsonl
```

#### Linux Questions (largest forum)
```bash
scrapy crawl linux_questions -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=2.0 -s CONCURRENT_REQUESTS_PER_DOMAIN=2 -o linuxquestions_output.jsonl
```

#### Arch Linux BBS
```bash
scrapy crawl arch_forums -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=2.0 -o arch_forums_output.jsonl
```

#### Ubuntu Forums
```bash
scrapy crawl ubuntu_forums -s CLOSESPIDER_ITEMCOUNT=0 -s DOWNLOAD_DELAY=2.5 -o ubuntu_forums_output.jsonl
```

### Scrapy with logging & stats

```bash
scrapy crawl lkml \
  -s CLOSESPIDER_ITEMCOUNT=0 \
  -s LOG_FILE=lkml_run.log \
  -s LOG_LEVEL=INFO \
  -s HTTPCACHE_ENABLED=True \
  -s AUTOTHROTTLE_ENABLED=True \
  -s AUTOTHROTTLE_TARGET_CONCURRENCY=4 \
  -o lkml_output.jsonl \
  2>&1 | tee lkml_console.log
```

### Run all Scrapy spiders in parallel (background)

```bash
mkdir -p scrapy_output logs
for spider in lkml lore_kernel debian_lists ubuntu_lists linux_questions arch_forums ubuntu_forums; do
    scrapy crawl $spider \
        -s CLOSESPIDER_ITEMCOUNT=0 \
        -s LOG_FILE=logs/${spider}.log \
        -o scrapy_output/${spider}.jsonl &
done
wait
echo "All spiders done"
```

---

## ═══ CRAWL4AI: Run Commands ═══

Run from `linux_scraper/crawl4ai_scraper/` directory.

### Run ALL sites (max crawl)

```bash
# All sites, site-default limits (~1.5M total docs potential)
python crawl4ai_linux.py --sites all --output-dir crawl4ai_output --concurrency 3

# All sites, unlimited per-site (WARNING: very long, may take days)
python crawl4ai_linux.py --sites all --output-dir crawl4ai_output --max-pages 999999999 --concurrency 3
```

### Individual site commands — MAX LIMIT

```bash
# LKML - max
python crawl4ai_linux.py --sites lkml --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Lore Kernel - max (13 subsystem lists)
python crawl4ai_linux.py --sites lore_kernel --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Debian Lists - max
python crawl4ai_linux.py --sites debian_lists --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Ubuntu Lists - max
python crawl4ai_linux.py --sites ubuntu_lists --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Linux Questions - max
python crawl4ai_linux.py --sites linux_questions --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Arch BBS - max
python crawl4ai_linux.py --sites arch_forums --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Ubuntu Forums - max
python crawl4ai_linux.py --sites ubuntu_forums --output-dir crawl4ai_output --max-pages 999999 --concurrency 1

# Arch Wiki (bonus)
python crawl4ai_linux.py --sites arch_wiki --output-dir crawl4ai_output --max-pages 10000 --concurrency 1

# Kernel Docs (bonus)
python crawl4ai_linux.py --sites kernel_docs --output-dir crawl4ai_output --max-pages 25000 --concurrency 1

# Man pages (bonus)
python crawl4ai_linux.py --sites man7 --output-dir crawl4ai_output --max-pages 5000 --concurrency 1
```

### Run multiple sites in parallel

```bash
python crawl4ai_linux.py \
    --sites lkml lore_kernel debian_lists ubuntu_lists \
    --output-dir crawl4ai_output \
    --max-pages 999999 \
    --concurrency 4

python crawl4ai_linux.py \
    --sites linux_questions arch_forums ubuntu_forums arch_wiki kernel_docs man7 \
    --output-dir crawl4ai_output \
    --max-pages 999999 \
    --concurrency 3
```

### Merge Crawl4AI outputs after crawling

```bash
python crawl4ai_linux.py --merge-only --output-dir crawl4ai_output
```

---

## ═══ POST-PROCESSING ═══

After running scrapers, merge and deduplicate everything:

```bash
# Merge all outputs from both scrapers
python postprocess/merge_and_deduplicate.py \
    --input-dirs scrapy_scraper/scrapy_output crawl4ai_scraper/crawl4ai_output \
    --output merged_linux_data/all_linux_rag_data.jsonl

# Stats are auto-saved to merged_linux_data/all_linux_rag_data_stats.json
```

---

## ═══ WGET Bonus Commands ═══

For simple static sites (man pages, docs), wget works perfectly:

```bash
# Kernel documentation (static HTML)
wget --recursive --level=5 --no-parent --accept "*.html" \
     --wait=1 --random-wait \
     --directory-prefix=wget_output/kernel_docs \
     https://docs.kernel.org/

# Man7 man pages
wget --recursive --level=3 --no-parent --accept "*.html" \
     --wait=1 --random-wait \
     --directory-prefix=wget_output/man7 \
     https://man7.org/linux/man-pages/

# Arch Wiki (all pages)
wget --recursive --level=3 --no-parent \
     --accept-regex ".*wiki\.archlinux\.org/title/[^:?]+" \
     --wait=2 --random-wait \
     --directory-prefix=wget_output/arch_wiki \
     https://wiki.archlinux.org/title/Special:AllPages
```

---

## ═══ Capacity Estimates ═══

| Engine | Site | Est. Docs |
|--------|------|-----------|
| Scrapy | lkml | 200K–500K |
| Scrapy | lore_kernel | 500K–1M |
| Scrapy | debian_lists | 200K–500K |
| Scrapy | ubuntu_lists | 100K–300K |
| Scrapy | linux_questions | 300K–800K |
| Scrapy | arch_forums | 100K–300K |
| Scrapy | ubuntu_forums | 200K–500K |
| Crawl4AI | All above | Similar |
| Crawl4AI | arch_wiki | 8K–12K |
| Crawl4AI | kernel_docs | 15K–25K |
| Crawl4AI | man7 | 4K–6K |
| **TOTAL** | | **~3–5M raw, ~1–2M unique** |

---

## Tips for Maximum Data

1. **Disable robots.txt** (already done): `ROBOTSTXT_OBEY = False`
2. **Use caching**: prevents re-scraping if interrupted
3. **Run Scrapy + Crawl4AI together**: they complement each other — Scrapy is faster, Crawl4AI handles JS-heavy pages
4. **Use a VPN or proxy rotation** if you get rate-limited (especially linuxquestions.org, ubuntuforums.org)
5. **Run on a server with good bandwidth** — this will generate 10–50 GB of data
6. **Use screen/tmux** to keep scraping alive in background:
   ```bash
   screen -S linux_scraper
   # then run commands inside screen
   # Ctrl+A, D to detach
   ```
