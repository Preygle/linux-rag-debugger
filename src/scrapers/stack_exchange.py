from stackapi import StackAPI
import os
import math
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

    def fetch_questions(self, site_name, tags, limit=50):
        """
        Fetches questions with accepted answers for given tags.
        Paginates properly to reach the requested limit.
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

        # Calculate pages needed (100 items per page)
        site.max_pages = max(1, math.ceil(limit / 100))
        print(f"Fetching from {site_name} for tags: {tags} (max_pages: {site.max_pages})...")

        try:
            # 1. Fetch questions with body
            questions = site.fetch('questions',
                                   tagged=tags,
                                   sort='votes',
                                   filter='withbody',
                                   min=1,
                                   )
            
            items = questions.get('items', [])
            print(f"Got {len(items)} questions from API.")

            if not items:
                print("No items found.")
                return []

            # 2. Collect accepted answer IDs
            questions_with_answers = [q for q in items if 'accepted_answer_id' in q]
            answer_ids = [q['accepted_answer_id'] for q in questions_with_answers]
            print(f"Questions with accepted answers: {len(questions_with_answers)}")
            
            # 3. Fetch answers in batches of 100
            answers_map = {}
            if answer_ids:
                for i in range(0, len(answer_ids), 100):
                    batch = answer_ids[i:i+100]
                    answers = site.fetch('answers', ids=batch, filter='withbody')
                    for ans in answers.get('items', []):
                        answers_map[ans['answer_id']] = ans.get('body', '')
                    print(f"Fetched answers batch {i//100 + 1}: {len(answers.get('items', []))} answers")

            # 4. Merge Q&A pairs
            processed = []
            for q in questions_with_answers:
                if len(processed) >= limit:
                    break
                    
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
                
            print(f"Total Q&A pairs extracted: {len(processed)}")
            return processed

        except Exception as e:
            print(f"Error fetching from {site_name}: {e}")
            import traceback
            traceback.print_exc()
            return []

if __name__ == "__main__":
    scraper = StackExchangeScraper()
    docs = scraper.fetch_questions('stackoverflow', ['linux', 'bash'], limit=5)
    print(f"Fetched {len(docs)} documents.")
    if docs:
        print(f"Sample: {docs[0]['title']}")
