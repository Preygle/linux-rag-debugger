"""
Post-processor: merge, deduplicate, validate, and stats for scraped JSONL files.
Usage: python postprocess.py [--input-dirs dir1 dir2 ...] [--output merged_output.jsonl]
"""

import json
import os
import re
import argparse
from collections import Counter
from datetime import datetime


REQUIRED_FIELDS = {"doc_id", "domain", "hardware_env", "problem", "raw_logs", "solution", "source_file", "original_link"}


def is_valid_doc(obj: dict) -> bool:
    if not REQUIRED_FIELDS.issubset(obj.keys()):
        return False
    if len(obj.get("problem", "")) < 30:
        return False
    url = obj.get("original_link", "")
    if not url.startswith("http"):
        return False
    return True


def clean_doc(obj: dict) -> dict:
    for field in ["problem", "solution", "raw_logs"]:
        if field in obj and obj[field]:
            obj[field] = re.sub(r"\s+", " ", str(obj[field])).strip()
            obj[field] = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", obj[field])
    return obj


def merge_jsonl_dirs(input_dirs: list, output_path: str, min_problem_len: int = 50):
    seen_ids = set()
    seen_problems = set()
    total_read = 0
    total_written = 0
    skipped_invalid = 0
    skipped_dup = 0
    domain_counter = Counter()
    site_counter = Counter()
    
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as fout:
        for d in input_dirs:
            if not os.path.isdir(d):
                # Try as a single file
                files = [d] if d.endswith(".jsonl") else []
            else:
                files = [os.path.join(d, f) for f in os.listdir(d) if f.endswith(".jsonl")]
            
            for fpath in sorted(files):
                print(f"  Processing: {fpath}")
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as fin:
                        for line in fin:
                            line = line.strip()
                            if not line:
                                continue
                            total_read += 1
                            try:
                                obj = json.loads(line)
                            except json.JSONDecodeError:
                                skipped_invalid += 1
                                continue
                            
                            if not is_valid_doc(obj):
                                skipped_invalid += 1
                                continue
                            
                            obj = clean_doc(obj)
                            
                            doc_id = obj.get("doc_id", "")
                            problem = obj.get("problem", "")[:100]
                            
                            if doc_id in seen_ids:
                                skipped_dup += 1
                                continue
                            
                            # Near-duplicate problem check
                            problem_key = re.sub(r"\W+", " ", problem.lower()).strip()
                            if problem_key in seen_problems and len(problem_key) > 20:
                                skipped_dup += 1
                                continue
                            
                            seen_ids.add(doc_id)
                            seen_problems.add(problem_key)
                            domain_counter[obj.get("domain", "unknown")] += 1
                            
                            # Track source site
                            url = obj.get("original_link", "")
                            host = url.split("/")[2] if "//" in url else "unknown"
                            site_counter[host] += 1
                            
                            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                            total_written += 1
                            
                            if total_written % 10000 == 0:
                                print(f"    Written: {total_written:,} docs")
                                
                except Exception as e:
                    print(f"  ERROR reading {fpath}: {e}")
    
    print(f"\n{'='*60}")
    print(f"MERGE COMPLETE")
    print(f"{'='*60}")
    print(f"Total read:        {total_read:,}")
    print(f"Total written:     {total_written:,}")
    print(f"Skipped (invalid): {skipped_invalid:,}")
    print(f"Skipped (dup):     {skipped_dup:,}")
    print(f"\nOutput: {output_path}")
    
    print(f"\nTop domains:")
    for domain, count in domain_counter.most_common(20):
        print(f"  {domain:<30} {count:>8,}")
    
    print(f"\nTop source sites:")
    for site, count in site_counter.most_common(15):
        print(f"  {site:<40} {count:>8,}")
    
    # Save stats
    stats = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_read": total_read,
        "total_written": total_written,
        "skipped_invalid": skipped_invalid,
        "skipped_dup": skipped_dup,
        "domains": dict(domain_counter.most_common()),
        "sites": dict(site_counter.most_common()),
    }
    stats_path = output_path.replace(".jsonl", "_stats.json")
    with open(stats_path, "w") as sf:
        json.dump(stats, sf, indent=2)
    print(f"\nStats saved: {stats_path}")
    
    return total_written


def main():
    parser = argparse.ArgumentParser(description="Merge & deduplicate scraped JSONL files")
    parser.add_argument("--input-dirs", nargs="+", required=True,
                        help="Directories or .jsonl files to merge")
    parser.add_argument("--output", default="merged_linux_data/all_linux_rag_data.jsonl",
                        help="Output merged JSONL file")
    parser.add_argument("--min-problem-len", type=int, default=50,
                        help="Minimum problem text length")
    args = parser.parse_args()
    
    print(f"\n📊 Post-processor: merge + deduplicate")
    print(f"Input dirs: {args.input_dirs}")
    print(f"Output:     {args.output}\n")
    
    merge_jsonl_dirs(args.input_dirs, args.output, args.min_problem_len)


if __name__ == "__main__":
    main()
