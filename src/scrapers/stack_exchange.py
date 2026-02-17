from stackapi import StackAPI
import os
import json
from dotenv import load_dotenv

load_dotenv()

class StackExchangeScraper:
    def __init__(self, key=None):
        self.key = key or os.getenv("STACK_API_KEY")
        # Initialize sites
        self.so = StackAPI('stackoverflow', key=self.key)
        self.sf = StackAPI('serverfault', key=self.key)
        self.ub = StackAPI('askubuntu', key=self.key)
        
        # Consistent settings
        for site in [self.so, self.sf, self.ub]:
            site.page_size = 100
            site.max_pages = 1

    def fetch_questions(self, site_name, tags, limit=50):
        """
        Fetches questions with accepted answers for given tags.
        """
        site = None
        if site_name == 'stackoverflow':
            site = self.so
        elif site_name == 'serverfault':
            site = self.sf
        elif site_name == 'askubuntu':
            site = self.ub
        else:
            print(f"Unknown site: {site_name}")
            return []

        print(f"Fetching from {site_name} for tags: {tags}...")
        try:
            questions = site.fetch('questions', 
                                   tagged=tags, 
                                   sort='votes', 
                                   filter='withbody', # Custom filter needed? 'withbody' includes body
                                   min=1, # At least 1 vote
                                  )
            
            # Filter for accepted answers
            # Note: The simple fetch above might not include answers. 
            # We usually need 'questions' with 'filter' that includes answers or make separate call.
            # Using a built-in filter '!9_bDDxJY5' which includes question body, answers, comments
            # Or construct a custom one. For simplicitly, let's just get questions first, 
            # then we might need to fetch answers if not included.
            
            # Actually, let's use a standard filter that includes answers body.
            # Filter '!*SU8CGYZITCB.D*(BDVIficKj7nFMLLDij64nVID)N9aK3GM' is a common extensive one.
            # But 'withbody' just gives question body.
            
            # Let's try to fetch relevant data.
            # We want: Title, Body, Accepted Answer Body.
            
            # 1. Fetch questions
            print(f"DEBUG: Fetching questions for tags {tags} from {site_name}...")
            questions = site.fetch('questions',
                                   tagged=tags,
                                   sort='votes',
                                   filter='withbody',
                                   min=1,
                                   pagesize=min(limit, 100)
                                   )
            
            items = questions.get('items', [])
            print(f"DEBUG: Got {len(items)} questions. Raw keys: {questions.keys()}")
            if 'error_id' in questions:
                 print(f"DEBUG: API Error: {questions.get('error_message')}")

            if not items:
                print("DEBUG: No items found.")
                return []

            # 2. Collect accepted answer IDs
            answer_ids = [q['accepted_answer_id'] for q in items if 'accepted_answer_id' in q]
            
            # 3. Fetch answers
            answers_map = {}
            if answer_ids:
                # Fetch in batches of 100 if needed, but stackapi handles batching usually
                answers = site.fetch('answers', ids=answer_ids, filter='withbody')
                for ans in answers.get('items', []):
                    answers_map[ans['answer_id']] = ans.get('body', '')

            # 4. Merge
            processed = []
            for q in items:
                if 'accepted_answer_id' not in q:
                    continue
                
                ans_body = answers_map.get(q['accepted_answer_id'])
                if not ans_body:
                    continue

                processed.append({
                    'source': f"{site_name}",
                    'id': q['question_id'],
                    'title': q['title'],
                    'question': q['body'],
                    'answer': ans_body,
                    'link': q['link'],
                    'tags': q['tags']
                })
                
            return processed

        except Exception as e:
            print(f"Error fetching from {site_name}: {e}")
            return []

if __name__ == "__main__":
    scraper = StackExchangeScraper()
    docs = scraper.fetch_questions('stackoverflow', ['linux', 'bash'], limit=5)
    print(f"Fetched {len(docs)} documents.")
    if docs:
        print(f"Sample: {docs[0]['title']}")
