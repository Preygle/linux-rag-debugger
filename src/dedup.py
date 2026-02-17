import hashlib
from typing import List, Dict, Set

class Deduplicator:
    def __init__(self):
        self.seen_hashes: Set[str] = set()

    def generate_hash(self, content: str) -> str:
        """
        Generates an MD5 hash of the content.
        """
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def is_duplicate(self, content: str) -> bool:
        """
        Checks if the content has been seen before.
        """
        content_hash = self.generate_hash(content)
        if content_hash in self.seen_hashes:
            return True
        self.seen_hashes.add(content_hash)
        return False

    def deduplicate_documents(self, documents: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Filters out duplicate documents from a list.
        """
        unique_docs = []
        duplicates = 0
        for doc in documents:
            if not self.is_duplicate(doc['content']):
                unique_docs.append(doc)
            else:
                duplicates += 1
        
        if duplicates > 0:
            print(f"Deduplication: Removed {duplicates} duplicate documents.")
        
        return unique_docs

if __name__ == "__main__":
    docs = [
        {'content': "hello world", 'metadata': {'id': 1}},
        {'content': "hello world", 'metadata': {'id': 2}},
        {'content': "foo bar", 'metadata': {'id': 3}}
    ]
    deduper = Deduplicator()
    unique = deduper.deduplicate_documents(docs)
    print(f"Original: {len(docs)}, Unique: {len(unique)}")
