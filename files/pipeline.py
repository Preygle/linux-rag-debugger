"""
LinuxLynx Data Pipeline Orchestrator
=====================================
Runs all scrapers, deduplicates, validates, and writes a final JSONL dataset.

Usage:
    python pipeline.py [--output OUTPUT] [--sources SOURCE ...] [--max MAX]
                       [--nvd-api-key KEY] [--log-level LEVEL]

Examples:
    # Full run (all sources, default limits)
    python pipeline.py --output dataset.jsonl

    # Only bug trackers and security sources
    python pipeline.py --sources bugzilla nvd syzkaller --output security.jsonl

    # Forums only, high doc count
    python pipeline.py --sources forums --max 500 --output forums.jsonl
"""

from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

from schema    import LinuxLynxDoc
from dedup     import Deduplicator

log = logging.getLogger("pipeline")


# ── Source registry ───────────────────────────────────────────────────────────

@dataclass
class SourceConfig:
    name: str
    label: str
    default_max: int
    enabled: bool = True


SOURCES_REGISTRY: list[SourceConfig] = [
    SourceConfig("bugzilla",   "Kernel Bugzilla",         max_docs := 200),
    SourceConfig("lkml",       "LKML / lore.kernel.org",  max_docs := 150),
    SourceConfig("forums",     "Linux Forums (SOLVED)",   max_docs := 200),
    SourceConfig("nvd",        "NVD CVE Database",        max_docs := 150),
    SourceConfig("syzkaller",  "Syzkaller Crash Reports", max_docs := 100),
    # Existing scrapers (already in codebase)
    SourceConfig("stack_exchange", "Stack Exchange (SO/SF/AU)", 300),
    SourceConfig("github",         "GitHub Issues",             200),
    SourceConfig("web_crawler",    "Arch Wiki",                 100),
]
max_docs = 200   # reset sentinel


def _load_scraper(source_name: str):
    """Lazily import a scraper module to avoid loading all deps upfront."""
    if source_name == "bugzilla":
        from scrapers.bugzilla_kernel import scrape
        return scrape
    elif source_name == "lkml":
        from scrapers.lkml import scrape
        return scrape
    elif source_name == "forums":
        from scrapers.forums import scrape
        return scrape
    elif source_name == "nvd":
        from scrapers.security import scrape_nvd
        return scrape_nvd
    elif source_name == "syzkaller":
        from scrapers.security import scrape_syzkaller
        return scrape_syzkaller
    elif source_name == "stack_exchange":
        try:
            from scrapers.stack_exchange import scrape
            return scrape
        except ImportError:
            log.warning("stack_exchange scraper not found")
            return None
    elif source_name == "github":
        try:
            from scrapers.github import scrape
            return scrape
        except ImportError:
            log.warning("github scraper not found")
            return None
    elif source_name == "web_crawler":
        try:
            from scrapers.web_crawler import scrape
            return scrape
        except ImportError:
            log.warning("web_crawler scraper not found")
            return None
    else:
        log.error("Unknown source: %s", source_name)
        return None


# ── Pipeline stats ────────────────────────────────────────────────────────────

class PipelineStats:
    def __init__(self):
        self.by_source:      dict[str, int] = {}
        self.by_domain:      dict[str, int] = {}
        self.by_confidence:  dict[str, int] = {}
        self.skipped_invalid = 0
        self.skipped_dedup   = 0
        self.total_accepted  = 0

    def record(self, doc: LinuxLynxDoc):
        self.total_accepted += 1
        self.by_source[doc.source]     = self.by_source.get(doc.source, 0) + 1
        self.by_domain[doc.domain]     = self.by_domain.get(doc.domain, 0) + 1
        self.by_confidence[doc.confidence] = self.by_confidence.get(doc.confidence, 0) + 1

    def report(self) -> str:
        lines = [
            "=" * 60,
            "LinuxLynx Pipeline Report",
            "=" * 60,
            f"  Total documents accepted : {self.total_accepted}",
            f"  Skipped (invalid schema) : {self.skipped_invalid}",
            f"  Skipped (dedup)          : {self.skipped_dedup}",
            "",
            "By source:",
        ]
        for src, n in sorted(self.by_source.items(), key=lambda x: -x[1]):
            lines.append(f"  {src:<30} {n:>5}")
        lines += ["", "By domain:"]
        for dom, n in sorted(self.by_domain.items(), key=lambda x: -x[1]):
            lines.append(f"  {dom:<30} {n:>5}")
        lines += ["", "By confidence:"]
        for conf, n in sorted(self.by_confidence.items()):
            lines.append(f"  {conf:<30} {n:>5}")
        lines.append("=" * 60)
        return "\n".join(lines)


# ── Core pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(
    sources:     list[str] | None = None,
    output_path: Path = Path("linuxlynx_dataset.jsonl"),
    max_per_source: int | None = None,
    fuzzy_dedup: bool = True,
    nvd_api_key: str | None = None,
) -> PipelineStats:
    """
    Run all configured scrapers, deduplicate, validate, and write output.

    Args:
        sources:         List of source names to run (None = all).
        output_path:     Destination JSONL file.
        max_per_source:  Override max docs per source.
        fuzzy_dedup:     Enable TF-IDF fuzzy deduplication (needs sklearn).
        nvd_api_key:     Optional NVD API key for higher rate limits.
    """
    stats    = PipelineStats()
    deduper  = Deduplicator(fuzzy=fuzzy_dedup)

    # Determine active sources
    active = [s for s in SOURCES_REGISTRY if sources is None or s.name in (sources or [])]
    if not active:
        log.error("No matching sources found for: %s", sources)
        return stats

    log.info("Pipeline starting. Sources: %s", [s.name for s in active])

    # Open output file for streaming writes
    output_path.parent.mkdir(parents=True, exist_ok=True)
    accepted_docs: list[LinuxLynxDoc] = []

    for src_cfg in active:
        if not src_cfg.enabled:
            log.info("Source %s disabled, skipping", src_cfg.name)
            continue

        scrape_fn = _load_scraper(src_cfg.name)
        if scrape_fn is None:
            continue

        max_docs = max_per_source or src_cfg.default_max

        log.info(
            "─── Running scraper: %s (max=%d) ───",
            src_cfg.label, max_docs
        )

        # Build kwargs for scrapers that accept extra params
        kwargs: dict = {"max_docs": max_docs}
        if src_cfg.name == "bugzilla":
            kwargs = {"max_bugs": max_docs}
        elif src_cfg.name == "lkml":
            kwargs = {"max_total": max_docs}
        elif src_cfg.name == "nvd":
            kwargs = {"max_docs": max_docs}
            if nvd_api_key:
                kwargs["api_key"] = nvd_api_key
        elif src_cfg.name == "syzkaller":
            kwargs = {"max_docs": max_docs}
        elif src_cfg.name in ("stack_exchange", "github", "web_crawler"):
            kwargs = {}   # use scraper defaults

        try:
            for doc in scrape_fn(**kwargs):
                # Validate schema
                errs = doc.validate()
                if errs:
                    log.debug("Validation failed for %s: %s", doc.doc_id, errs)
                    stats.skipped_invalid += 1
                    continue

                # Dedup
                accepted = deduper.add(doc)
                if not accepted:
                    stats.skipped_dedup += 1
                    continue

                stats.record(doc)
                accepted_docs.append(doc)
                log.info(
                    "  [+] %s | %s | %s | %s",
                    doc.doc_id, doc.domain, doc.confidence, doc.problem[:55]
                )

        except Exception as e:
            log.exception("Scraper %s raised an error: %s", src_cfg.name, e)

    # Final fuzzy dedup pass (removes near-duplicates across sources)
    log.info("Running final dedup pass (%d docs)...", len(accepted_docs))
    final_docs = deduper.unique_docs()
    dupes_removed = len(accepted_docs) - len(final_docs)
    if dupes_removed > 0:
        stats.skipped_dedup += dupes_removed
        stats.total_accepted = len(final_docs)
        log.info("  Fuzzy dedup removed %d additional near-duplicates", dupes_removed)

    # Write output
    log.info("Writing %d documents to %s", len(final_docs), output_path)
    with open(output_path, "w", encoding="utf-8") as fh:
        for doc in final_docs:
            fh.write(doc.to_jsonl() + "\n")

    log.info(stats.report())
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="LinuxLynx data collection pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--output", "-o", default="linuxlynx_dataset.jsonl",
        help="Output JSONL file path (default: linuxlynx_dataset.jsonl)",
    )
    p.add_argument(
        "--sources", "-s", nargs="+",
        choices=[s.name for s in SOURCES_REGISTRY],
        help="Which sources to run (default: all)",
    )
    p.add_argument(
        "--max", "-m", type=int, default=None,
        help="Max documents per source (overrides per-source defaults)",
    )
    p.add_argument(
        "--no-fuzzy-dedup", action="store_true",
        help="Disable TF-IDF fuzzy deduplication (faster but less thorough)",
    )
    p.add_argument(
        "--nvd-api-key", default=None,
        help="NVD API key for higher rate limits (50 req/30s vs 5 req/30s)",
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    logging.basicConfig(
        level   = getattr(logging, args.log_level),
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%H:%M:%S",
        stream  = sys.stderr,
    )

    stats = run_pipeline(
        sources        = args.sources,
        output_path    = Path(args.output),
        max_per_source = args.max,
        fuzzy_dedup    = not args.no_fuzzy_dedup,
        nvd_api_key    = args.nvd_api_key,
    )

    print(stats.report())
    print(f"\n✓ Dataset written to: {args.output}")
