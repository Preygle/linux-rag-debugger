import os
import argparse
import json
from typing import List, Dict

import sys

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

# Try imports with explicit error handling
def _try_import_scrapers():
    """
    Try to import scrapers from both possible path roots and return a dict of
    callables.  Each import is isolated so a failure in one scraper doesn't
    shadow the others.
    """
    scrapers = {}

    # Determine the two candidate import prefixes.
    prefixes = ["src.scrapers", "scrapers"]

    for name, attr in [
        ("StackExchangeScraper", ("stack_exchange", "StackExchangeScraper")),
        ("GitHubScraper",        ("github",          "GitHubScraper")),
        ("WebCrawler",           ("web_crawler",     "WebCrawler")),
        ("scrape_bugzilla",      ("bugzilla_kernel", "scrape")),
        ("scrape_lkml",          ("lkml",            "scrape")),
        ("scrape_forums",        ("forums",          "scrape")),
        ("scrape_nvd",           ("security",        "scrape_nvd")),
        ("scrape_syzkaller",     ("security",        "scrape_syzkaller")),
    ]:
        module_suffix, attr_name = attr
        for prefix in prefixes:
            try:
                import importlib
                mod = importlib.import_module(f"{prefix}.{module_suffix}")
                scrapers[name] = getattr(mod, attr_name)
                break
            except (ImportError, AttributeError):
                continue
        if name not in scrapers:
            print(f"WARNING: Could not import {name} from any known path.")

    return scrapers

_SCRAPERS = _try_import_scrapers()

StackExchangeScraper = _SCRAPERS.get("StackExchangeScraper", type("StackExchangeScraper", (), {}))
GitHubScraper        = _SCRAPERS.get("GitHubScraper",        type("GitHubScraper",        (), {}))
WebCrawler           = _SCRAPERS.get("WebCrawler",           type("WebCrawler",           (), {}))

def scrape_bugzilla(*a, **kw):
    fn = _SCRAPERS.get("scrape_bugzilla")
    return fn(*a, **kw) if fn else []

def scrape_lkml(*a, **kw):
    fn = _SCRAPERS.get("scrape_lkml")
    return fn(*a, **kw) if fn else []

def scrape_forums(*a, **kw):
    fn = _SCRAPERS.get("scrape_forums")
    return fn(*a, **kw) if fn else []

def scrape_nvd(*a, **kw):
    fn = _SCRAPERS.get("scrape_nvd")
    return fn(*a, **kw) if fn else []

def scrape_syzkaller(*a, **kw):
    fn = _SCRAPERS.get("scrape_syzkaller")
    return fn(*a, **kw) if fn else []

def load_documents(source_path: str = None, source_type: str = 'local', limit: int = 10, skip_dedup: bool = False) -> List[Dict[str, str]]:
    """
    Loads text documents from a directory or scraper.
    """
    documents = []
    
    # LOCAL FILE LOADING
    if source_type == 'local':
        if not source_path or not os.path.exists(source_path):
            print(f"Directory not found: {source_path}")
            return documents

        for root, _, files in os.walk(source_path):
            for file in files:
                if file.endswith(('.txt', '.md', '.log')):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            text = f.read()
                        documents.append({
                            'content': text,
                            'metadata': {'source': file_path}
                        })
                    except Exception as e:
                        print(f"Error loading {file_path}: {e}")

    # JSONL FILE LOADING (Previously Exported Data)
    elif source_type == 'jsonl':
        if not source_path or not os.path.exists(source_path):
            print(f"Directory not found: {source_path}")
            return documents

        for root, _, files in os.walk(source_path):
            for file in files:
                if file.endswith('.jsonl'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            for line in f:
                                if not line.strip(): continue
                                data = json.loads(line)
                                
                                # Handle SFT (Conversational) format
                                if 'messages' in data:
                                    messages = data['messages']
                                    if len(messages) >= 2:
                                        content = f"Question:\n{messages[0]['content']}\n\nAnswer:\n{messages[1]['content']}"
                                        documents.append({
                                            'content': content,
                                            'metadata': {'source': file_path, 'type': data.get('source', 'jsonl_sft')}
                                        })
                                # Handle RAG-Extracted schema (Our local/groq/normal formatters)
                                elif 'problem' in data and 'solution' in data:
                                    parts = []
                                    if data.get('domain'): parts.append(f"Domain: {data['domain']}")
                                    if data.get('hardware_env'): parts.append(f"Environment: {data['hardware_env']}")
                                    parts.append(f"Problem:\n{data['problem']}")
                                    if data.get('raw_logs'): parts.append(f"Logs:\n{data['raw_logs']}")
                                    parts.append(f"Solution:\n{data['solution']}")
                                    
                                    content = "\n\n".join(parts)
                                    documents.append({
                                        'content': content,
                                        'metadata': {
                                            'source': file_path, 
                                            'type': 'rag_extracted',
                                            'doc_id': data.get('doc_id', '')
                                        }
                                    })
                                # Handle Raw Pre-training format
                                elif 'text' in data:
                                    documents.append({
                                        'content': data['text'],
                                        'metadata': {'source': file_path, 'type': data.get('source', 'jsonl_raw')}
                                    })
                    except Exception as e:
                        print(f"Error loading JSONL {file_path}: {e}")
    
    # STACK EXCHANGE LOADING
    elif source_type == 'stackexchange':
        scraper = StackExchangeScraper()
        # Default tags for now
        tags = ['linux', 'bash', 'systemd'] 
        print(f"Scraping StackExchange for tags: {tags}")
        items = scraper.fetch_questions('stackoverflow', tags, limit=limit)
        for item in items:
            # Combine Q&A into a single document
            content = f"Title: {item['title']}\n\nQuestion:\n{item['question']}\n\nAnswer:\n{item['answer']}"
            documents.append({
                'content': content,
                'metadata': {
                    'source': item['link'], 
                    'type': 'stack_exchange', 
                    'tags': item['tags'],
                    'question': item['question'],
                    'answer': item['answer'],
                    'title': item['title']
                }
            })

    # GITHUB LOADING
    elif source_type == 'github':
        scraper = GitHubScraper()
        # Default repos for now - user can ideally pass this as arg
        repos = ['systemd/systemd', 'torvalds/linux'] # Linux might be too huge/noisy but let's try
        for repo in repos:
            print(f"Scraping GitHub repo: {repo}")
            items = scraper.fetch_issues(repo, limit=limit)
            for item in items:
                documents.append({
                    'content': item['content'],
                    'metadata': {'source': item['link'], 'type': 'github_issue'}
                })

    # WEB CRAWLER LOADING
    elif source_type == 'web':
        crawler = WebCrawler()
        # Default start URL
        start_url = "https://wiki.archlinux.org/title/Category:System_administration"
        print(f"Crawling Web: {start_url}")
        items = crawler.crawl_arch_wiki(limit=limit)
        for item in items:
            documents.append({
                'content': item['content'],
                'metadata': {'source': item['source'], 'title': item['title'], 'type': 'web_page'}
            })

    # EXPERT SCRAPERS
    elif source_type == 'bugzilla':
        print(f"Scraping Kernel Bugzilla (limit {limit})")
        for doc in scrape_bugzilla(max_bugs=limit):
            sys.stdout.write(f"\rFetched {len(documents)+1} Bugzilla docs...")
            sys.stdout.flush()
            doc_dict = json.loads(doc.to_jsonl())
            content = f"Domain: {doc_dict.get('domain', '')}\nProblem:\n{doc.problem}\n\nLogs:\n{doc.raw_logs}\n\nDebug Steps:\n{doc.debug_steps}\n\nRoot Cause:\n{doc.root_cause}\n\nSolution:\n{doc.solution}\n\nReasoning:\n{doc.reasoning}"
            documents.append({
                'content': content,
                'metadata': {'source': doc.link, 'type': 'bugzilla', 'doc_id': doc.doc_id, 'failure_type': doc.failure_type}
            })
        print()

    elif source_type == 'lkml':
        print(f"Scraping LKML (limit {limit})")
        for doc in scrape_lkml(max_total=limit):
            sys.stdout.write(f"\rFetched {len(documents)+1} LKML threads...")
            sys.stdout.flush()
            doc_dict = json.loads(doc.to_jsonl())
            content = f"Domain: {doc_dict.get('domain', '')}\nProblem:\n{doc.problem}\n\nLogs:\n{doc.raw_logs}\n\nDebug Steps:\n{doc.debug_steps}\n\nRoot Cause:\n{doc.root_cause}\n\nSolution:\n{doc.solution}\n\nReasoning:\n{doc.reasoning}"
            documents.append({
                'content': content,
                'metadata': {'source': doc.link, 'type': 'lkml', 'doc_id': doc.doc_id, 'failure_type': doc.failure_type}
            })
        print()

    elif source_type == 'forums':
        print(f"Scraping Forums (limit {limit})")
        for doc in scrape_forums(max_docs=limit):
            sys.stdout.write(f"\rFetched {len(documents)+1} Forum posts...")
            sys.stdout.flush()
            doc_dict = json.loads(doc.to_jsonl())
            content = f"Domain: {doc_dict.get('domain', '')}\nProblem:\n{doc.problem}\n\nLogs:\n{doc.raw_logs}\n\nDebug Steps:\n{doc.debug_steps}\n\nRoot Cause:\n{doc.root_cause}\n\nSolution:\n{doc.solution}\n\nReasoning:\n{doc.reasoning}"
            documents.append({
                'content': content,
                'metadata': {'source': doc.link, 'type': 'forums', 'doc_id': doc.doc_id, 'failure_type': doc.failure_type}
            })
        print()

    elif source_type == 'nvd':
        print(f"Scraping NVD CVE Database (limit {limit})")
        api_key = os.getenv("NVD_API_KEY")
        for doc in scrape_nvd(max_docs=limit, api_key=api_key):
            sys.stdout.write(f"\rFetched {len(documents)+1} CVE records...")
            sys.stdout.flush()
            doc_dict = json.loads(doc.to_jsonl())
            content = f"Domain: {doc_dict.get('domain', '')}\nProblem:\n{doc.problem}\n\nLogs:\n{doc.raw_logs}\n\nDebug Steps:\n{doc.debug_steps}\n\nRoot Cause:\n{doc.root_cause}\n\nSolution:\n{doc.solution}\n\nReasoning:\n{doc.reasoning}"
            documents.append({
                'content': content,
                'metadata': {'source': doc.link, 'type': 'nvd', 'doc_id': doc.doc_id, 'failure_type': doc.failure_type}
            })
        print()

    elif source_type == 'syzkaller':
        print(f"Scraping Syzbot Crash Repository (limit {limit})")
        for doc in scrape_syzkaller(max_docs=limit):
            sys.stdout.write(f"\rFetched {len(documents)+1} Syzbot crashes...")
            sys.stdout.flush()
            doc_dict = json.loads(doc.to_jsonl())
            content = f"Domain: {doc_dict.get('domain', '')}\nProblem:\n{doc.problem}\n\nLogs:\n{doc.raw_logs}\n\nDebug Steps:\n{doc.debug_steps}\n\nRoot Cause:\n{doc.root_cause}\n\nSolution:\n{doc.solution}\n\nReasoning:\n{doc.reasoning}"
            documents.append({
                'content': content,
                'metadata': {'source': doc.link, 'type': 'syzkaller', 'doc_id': doc.doc_id, 'failure_type': doc.failure_type}
            })
        print()

    # Deduplication (skip for pre-processed JSONL — that data is already curated)
    if skip_dedup or source_type == 'jsonl':
        print(f"Total documents loaded: {len(documents)} (deduplication skipped for pre-processed data)")
        return documents

    print(f"Total documents before deduplication: {len(documents)}")
    deduper = Deduplicator()
    unique_documents = deduper.deduplicate_documents(documents)
    print(f"Total documents after deduplication: {len(unique_documents)}")

    return unique_documents

try:
    from src.dedup import Deduplicator
except ImportError:
    try:
        from dedup import Deduplicator
    except ImportError:
        print("WARNING: Could not import Deduplicator.")
        class Deduplicator:
            def deduplicate_documents(self, docs): return docs
if __name__ == '__main__':
    docs = load_documents(source_type='stackexchange', limit=2)
    print(f'Loaded {len(docs)} documents.')
