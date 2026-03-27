"""
LinuxLynx deduplication pipeline.

Strategy (applied in order):
  1. Exact hash  — SHA-256 of normalized (problem + raw_logs + solution)
  2. Fuzzy sim   — TF-IDF cosine similarity > 0.90 → keep higher confidence
  3. Cross-source — bug tracker entry beats forum entry for same bug
"""

from __future__ import annotations
import json
import sys
from pathlib import Path
from typing import Iterable

from schema import LinuxLynxDoc, CONFIDENCES

# Optional: scikit-learn for fuzzy dedup
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False

# Source-quality ranking for cross-source preference
_SOURCE_RANK = {
    "bugzilla": 0,
    "lkml": 1,
    "stackoverflow": 2,
    "serverfault": 2,
    "askubuntu": 2,
    "forum": 3,
    "web": 4,
}

def _source_rank(source: str) -> int:
    if source.startswith("github"):
        return 1
    return _SOURCE_RANK.get(source, 5)

def _confidence_rank(conf: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(conf, 3)

def _prefer(a: LinuxLynxDoc, b: LinuxLynxDoc) -> LinuxLynxDoc:
    """Return the preferred document between two near-duplicates."""
    ra, rb = _source_rank(a.source), _source_rank(b.source)
    if ra != rb:
        return a if ra < rb else b
    ca, cb = _confidence_rank(a.confidence), _confidence_rank(b.confidence)
    return a if ca <= cb else b


class Deduplicator:
    """
    Stateful deduplicator. Feed documents one-by-one via `add()`,
    then call `unique_docs()` to get the deduplicated list.
    """

    FUZZY_THRESHOLD = 0.90

    def __init__(self, fuzzy: bool = True):
        self._seen_hashes: dict[str, LinuxLynxDoc] = {}  # hash → doc
        self._docs: list[LinuxLynxDoc] = []
        self._fuzzy = fuzzy and _HAS_SKLEARN

    # ── Public API ────────────────────────────────────────────────────────────

    def add(self, doc: LinuxLynxDoc) -> bool:
        """
        Add a document. Returns True if accepted, False if dropped as duplicate.
        """
        h = doc.content_hash()

        # Step 1: exact hash dedup
        if h in self._seen_hashes:
            existing = self._seen_hashes[h]
            preferred = _prefer(existing, doc)
            self._seen_hashes[h] = preferred
            # Replace in list too
            for i, d in enumerate(self._docs):
                if d.content_hash() == h:
                    self._docs[i] = preferred
                    break
            return False

        self._seen_hashes[h] = doc
        self._docs.append(doc)
        return True

    def unique_docs(self) -> list[LinuxLynxDoc]:
        """
        Return deduplicated docs. If sklearn available, also applies
        fuzzy TF-IDF dedup pass.
        """
        if not self._fuzzy or len(self._docs) < 2:
            return list(self._docs)
        return self._fuzzy_pass(self._docs)

    # ── Fuzzy pass ────────────────────────────────────────────────────────────

    def _fuzzy_pass(self, docs: list[LinuxLynxDoc]) -> list[LinuxLynxDoc]:
        corpus = [
            f"{d.problem} {d.raw_logs[:500]} {d.solution}"
            for d in docs
        ]
        vec = TfidfVectorizer(
            min_df=1, ngram_range=(1, 2), max_features=20_000, sublinear_tf=True
        )
        try:
            tfidf = vec.fit_transform(corpus)
        except ValueError:
            return docs   # corpus too small / empty

        n = len(docs)
        drop: set[int] = set()

        # Process in batches of 500 to avoid O(n²) memory blow-up
        batch = 500
        for start in range(0, n, batch):
            end = min(start + batch, n)
            sim = cosine_similarity(tfidf[start:end], tfidf).toarray()
            for i, row in enumerate(sim):
                gi = start + i
                if gi in drop:
                    continue
                for gj in range(gi + 1, n):
                    if gj in drop:
                        continue
                    if row[gj] >= self.FUZZY_THRESHOLD:
                        loser = _prefer(docs[gi], docs[gj])
                        drop.add(gi if loser is docs[gi] else gj)

        return [d for i, d in enumerate(docs) if i not in drop]


# ── CLI helper ────────────────────────────────────────────────────────────────

def dedup_jsonl_file(path: Path, out_path: Path | None = None) -> int:
    """
    Dedup a .jsonl file in-place (or to out_path).
    Returns number of documents removed.
    """
    deduper = Deduplicator()
    raw_docs: list[LinuxLynxDoc] = []

    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                doc = LinuxLynxDoc(**data)
                raw_docs.append(doc)
                deduper.add(doc)
            except Exception as e:
                print(f"[dedup] skip malformed line: {e}", file=sys.stderr)

    unique = deduper.unique_docs()
    removed = len(raw_docs) - len(unique)

    dest = out_path or path
    with open(dest, "w") as fh:
        for doc in unique:
            fh.write(doc.to_jsonl() + "\n")

    print(f"[dedup] {len(raw_docs)} in → {len(unique)} out ({removed} removed)")
    return removed


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dedup.py <input.jsonl> [output.jsonl]")
        sys.exit(1)
    inp = Path(sys.argv[1])
    outp = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    dedup_jsonl_file(inp, outp)
