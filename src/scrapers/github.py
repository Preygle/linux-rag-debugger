from github import Github, GithubException
import os
import time
from dotenv import load_dotenv

load_dotenv()

class GitHubScraper:
    def __init__(self, token=None):
        self.token = token or os.getenv("GITHUB_ACCESS_TOKEN") or os.getenv("GITHUB_TOKEN")
        try:
            self.g = Github(self.token, timeout=30)
            if self.token:
                try:
                    self.g.get_user().login
                    rate = self.g.get_rate_limit()
                    remaining = getattr(rate.core, 'remaining', None) or getattr(rate, 'rate', None)
                    if remaining is not None:
                        print(f"GitHub API authenticated. Rate: {remaining}")
                    else:
                        print("GitHub API authenticated.")
                except GithubException as e:
                    if e.status == 401:
                        print("GitHub token rejected, falling back to unauthenticated access.")
                        self.token = None
                        self.g = Github(timeout=30)
                    else:
                        raise
            else:
                print("GitHub API client created without authentication.")
        except Exception as e:
            print(f"Error initializing GitHub client: {e}")
            self.g = None

    def fetch_issues(self, repo_name, labels=None, limit=50):
        """
        Fetches closed issues with optional label filter.
        Includes robust error handling and progress logging.
        """
        if not self.g:
            print(f"GitHub client not initialized, skipping {repo_name}")
            return []
            
        print(f"Fetching issues from {repo_name} (limit: {limit})...")
        try:
            repo = self.g.get_repo(repo_name)
            if labels:
                issues = repo.get_issues(state='closed', labels=labels, sort='comments', direction='desc')
            else:
                issues = repo.get_issues(state='closed', sort='comments', direction='desc')
            
            processed = []
            count = 0
            skipped_prs = 0
            errors = 0
            
            for issue in issues:
                if count >= limit:
                    break
                    
                # Skip pull requests
                if issue.pull_request:
                    skipped_prs += 1
                    continue
                
                try:
                    # Get conversation
                    comments = issue.get_comments()
                    discussion = f"Title: {issue.title}\n\nBody:\n{issue.body or '(no body)'}\n\n"
                    
                    # Limit to first 5 comments
                    comment_count = 0
                    for comment in comments:
                        if comment_count >= 5:
                            break
                        discussion += f"--- Comment by {comment.user.login} ---\n{comment.body}\n"
                        comment_count += 1
                    
                    processed.append({
                        'source': f"github_{repo_name}",
                        'id': str(issue.number),
                        'title': issue.title,
                        'content': discussion,
                        'link': issue.html_url
                    })
                    count += 1
                    
                    if count % 10 == 0:
                        print(f"  Progress: {count}/{limit} issues fetched")
                    
                except GithubException as e:
                    if e.status == 403:
                        print(f"  Rate limited! Waiting 60s...")
                        time.sleep(60)
                    else:
                        print(f"  Error on issue #{issue.number}: {e}")
                        errors += 1
                except Exception as e:
                    print(f"  Error on issue #{issue.number}: {e}")
                    errors += 1
                    if errors > 10:
                        print("  Too many errors, stopping.")
                        break
            
            print(f"Fetched {len(processed)} issues from {repo_name} (skipped {skipped_prs} PRs, {errors} errors)")
            return processed
            
        except GithubException as e:
            print(f"GitHub API error for {repo_name}: {e.status} - {e.data}")
            return []
        except Exception as e:
            print(f"Error fetching from {repo_name}: {e}")
            import traceback
            traceback.print_exc()
            return []

if __name__ == "__main__":
    scraper = GitHubScraper()
    docs = scraper.fetch_issues('systemd/systemd', limit=3)
    print(f"Fetched {len(docs)} issues.")
    if docs:
        print(f"Sample: {docs[0]['title']}")
