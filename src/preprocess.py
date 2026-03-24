import re
from typing import Dict, List

class QueryPreprocessor:
    def __init__(self):
        # Regex patterns for common Linux entities
        self.patterns = {
            'error_code': r'error code \d+|exit code \d+|errno \d+|SIG\w+',
            'file_path': r'(/[\w\-\.]+)+',
            'library': r'lib[\w\-\.]+\.so(\.[\d\.]+)?',
            'command': r'`([^`]+)`',
            'package': r'package [\w\-\+]+|apt install [\w\-\+]+'
        }

    def extract_entities(self, query: str) -> Dict[str, List[str]]:
        """
        Extracts structured entities from the query.
        """
        entities = {}
        for key, pattern in self.patterns.items():
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                 # Flatten if groups are returned
                cleaned_matches = []
                for m in matches:
                    if isinstance(m, tuple):
                        cleaned_matches.extend([x for x in m if x])
                    else:
                        cleaned_matches.append(m)
                entities[key] = list(set(cleaned_matches))
        return entities

    def enhance_query(self, query: str) -> str:
        """
        Enhances the query by emphasizing extracted entities.
        Example: "error loading libssl.so" -> "error loading libssl.so KEYWORDS: libssl.so"
        """
        entities = self.extract_entities(query)
        keywords = []
        for key, values in entities.items():
            keywords.extend(values)
        
        if keywords:
            return f"{query}\n\nCritical Entities: {', '.join(keywords)}"
        return query

if __name__ == "__main__":
    qp = QueryPreprocessor()
    q = "I got a segmentation fault in /usr/bin/python3 when loading libssl.so.1.1"
    entities = qp.extract_entities(q)
    print(f"Entities: {entities}")
    enhanced = qp.enhance_query(q)
    print(f"Enhanced Query: {enhanced}")
