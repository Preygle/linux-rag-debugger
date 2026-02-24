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
try:
    # Try importing as if running from project root (e.g. python main.py)
    # This expects src to be a package or in path
    try:
        from src.scrapers.stack_exchange import StackExchangeScraper
        from src.scrapers.github import GitHubScraper
        from src.scrapers.web_crawler import WebCrawler
        from src.dedup import Deduplicator
    except ImportError:
        # Fallback if src is in path but not as a top-level package (e.g. python src/ingest.py)
        from scrapers.stack_exchange import StackExchangeScraper
        from scrapers.github import GitHubScraper
        from scrapers.web_crawler import WebCrawler
        from dedup import Deduplicator
except ImportError as e:
    print(f"CRITICAL: Failed to import scraper or dedup modules: {e}")
    # Define dummy classes to prevent NameError if imports fail
    class StackExchangeScraper: pass
    class GitHubScraper: pass
    class WebCrawler: pass
    class Deduplicator: 
        def deduplicate_documents(self, docs): return docs

def load_documents(source_path: str = None, source_type: str = 'local', limit: int = 10) -> List[Dict[str, str]]:
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

    # Deduplication
    print(f"Total documents before deduplication: {len(documents)}")
    deduper = Deduplicator()
    unique_documents = deduper.deduplicate_documents(documents)
    print(f"Total documents after deduplication: {len(unique_documents)}")

    return unique_documents

if __name__ == "__main__":
    # Test
    docs = load_documents(source_type='stackexchange', limit=2)
    print(f"Loaded {len(docs)} documents.")
