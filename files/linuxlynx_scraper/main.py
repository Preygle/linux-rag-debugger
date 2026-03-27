#!/usr/bin/env python3
"""
LinuxLynx Scraper — main entry point
=====================================
Usage:
  python main.py --source bugzilla   --limit 50  --out data/bugzilla.jsonl
  python main.py --source lkml       --limit 100 --out data/lkml.jsonl
  python main.py --source forums     --limit 50  --out data/forums.jsonl
  python main.py --source nvd        --limit 200 --out data/nvd.jsonl
  python main.py --source syzkaller  --limit 100 --out data/syzkaller.jsonl
  python main.py --source all        --limit 50  --out data/

Available sources: bugzilla, lkml, forums, nvd, syzkaller, all
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from src.dedup import Deduplicator

log = logging.getLogger(__name__)

SOURCES = ["bugzilla", "lkml", "forums", "nvd", "syzkaller"]


def _get_scraper(source: str):
    if source == "bugzilla":
        from src.scrapers.bugzilla_kernel import scrape
        return scrape
    if source == "lkml":
        from src.scrapers.lkml import scrape
        return scrape
    if source == "forums":
        from src.scrapers.forums import scrape
        return scrape
    if source == "nvd":
        from src.scrapers.security import scrape_nvd
        return scrape_nvd
    if source == "syzkaller":
        from src.scrapers.security import scrape_syzkaller
        return scrape_syzkaller
    raise ValueError(f"Unknown source: {source!r}")


def run_scraper(source: str, limit: int, out_path: str):
    print(f"\n{'='*60}")
    print(f"  Source  : {source}")
    print(f"  Limit   : {limit}")
    print(f"  Output  : {out_path}")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    deduper = Deduplicator()
    scraper = _get_scraper(source)

    # Build kwargs based on source
    if source == "bugzilla":
        gen = scraper(max_bugs=limit)
    elif source == "lkml":
        gen = scraper(max_total=limit)
    elif source == "forums":
        gen = scraper(max_docs=limit)
    elif source == "nvd":
        gen = scraper(max_docs=limit, api_key=os.getenv("NVD_API_KEY"))
    elif source == "syzkaller":
        gen = scraper(max_docs=limit)
    else:
        gen = scraper(max_docs=limit)

    count = 0
    dup_count = 0

    with open(out_path, "w", encoding="utf-8") as fh:
        for doc in gen:
            content_hash_input = doc.problem + doc.raw_logs + doc.solution
            if deduper.is_duplicate(content_hash_input):
                dup_count += 1
                log.debug("DUP: %s", doc.doc_id)
                continue
            fh.write(doc.to_jsonl() + "\n")
            count += 1
            print(f"  [{count:>4}] {doc.doc_id}: {doc.problem[:65]}")

        deduper._save_hashes()

    print(f"\n  ✓ Wrote {count} documents ({dup_count} duplicates skipped) → {out_path}\n")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="LinuxLynx data scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source", "-s",
        required=True,
        choices=SOURCES + ["all"],
        help="Data source to scrape",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=50,
        help="Maximum documents to collect per source (default: 50)",
    )
    parser.add_argument(
        "--out", "-o",
        default="data/",
        help="Output .jsonl file, or a directory when --source=all (default: data/)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stdout,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if args.source == "all":
        # Output must be a directory
        out_dir = args.out.rstrip("/")
        total = 0
        for src in SOURCES:
            out_file = os.path.join(out_dir, f"{src}.jsonl")
            try:
                total += run_scraper(src, args.limit, out_file)
            except Exception as e:
                print(f"  ✗ {src} failed: {e}", file=sys.stderr)
        print(f"\nTotal documents collected: {total}")
    else:
        # Single source — if out is a directory, append filename
        out = args.out
        if os.path.isdir(out) or out.endswith("/"):
            out = os.path.join(out.rstrip("/"), f"{args.source}.jsonl")
        run_scraper(args.source, args.limit, out)


if __name__ == "__main__":
    main()
