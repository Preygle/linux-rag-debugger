from github import Github
import os
from dotenv import load_dotenv

load_dotenv()

class GitHubScraper:
    def __init__(self, token=None):
        self.token = token or os.getenv("GITHUB_ACCESS_TOKEN")
        try:
            self.g = Github(self.token)
        except Exception as e:
            print(f"Error initializing GitHub client: {e}")
            self.g = None

    def fetch_issues(self, repo_name, labels=['bug'], limit=50):
        """
        Fetches closed issues with specific labels.
        """
        if not self.g:
            return []
            
        print(f"Fetching issues from {repo_name}...")
        try:
            repo = self.g.get_repo(repo_name)
            issues = repo.get_issues(state='closed', labels=labels, sort='comments', direction='desc')
            
            processed = []
            count = 0
            for issue in issues:
                if count >= limit:
                    break
                    
                # Skip pull requests
                if issue.pull_request:
                    continue
                
                # Get conversation
                comments = issue.get_comments()
                discussion = f"Title: {issue.title}\n\nBody:\n{issue.body}\n\n"
                
                # Heuristic: combine top comments as "resolution" context
                for comment in comments[:5]: # limit to first 5 comments to avoid huge text
                    discussion += f"--- Comment by {comment.user.login} ---\n{comment.body}\n"
                
                processed.append({
                    'source': f"github_{repo_name}",
                    'id': str(issue.number),
                    'title': issue.title,
                    'content': discussion,
                    'link': issue.html_url
                })
                count += 1
                
            return processed
            
        except Exception as e:
            print(f"Error fetching from {repo_name}: {e}")
            return []

if __name__ == "__main__":
    scraper = GitHubScraper()
    docs = scraper.fetch_issues('systemd/systemd', limit=3)
    print(f"Fetched {len(docs)} issues.")
    if docs:
        print(f"Sample: {docs[0]['title']}")
