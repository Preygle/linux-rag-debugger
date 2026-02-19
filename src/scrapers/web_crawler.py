import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin, urlparse

class WebCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LinuxRAGBot/1.0 (Educational Project)'
        })

    def scrape_url(self, url, retries=3):
        """
        Scrapes a single URL with retry logic on timeout.
        """
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    print(f"  Failed {url}: HTTP {response.status_code}")
                    return None
                
                soup = BeautifulSoup(response.content, 'lxml')
                
                # Remove script and style elements
                for script in soup(["script", "style", "nav", "footer", "header"]):
                    script.decompose()
                
                # Extract title
                title = soup.title.string if soup.title else "No Title"
                
                # Extract main content (Arch Wiki uses 'mw-content-text')
                content_div = soup.find('div', {'id': 'mw-content-text'})
                if not content_div:
                    content_div = soup.find('main')
                if not content_div:
                    content_div = soup.body
                    
                text = content_div.get_text(separator='\n')
                
                # Cleanup empty lines
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                cleaned_text = '\n'.join(lines)
                
                return {
                    'source': url,
                    'title': title,
                    'content': cleaned_text
                }
                
            except requests.exceptions.Timeout:
                print(f"  Timeout on {url} (attempt {attempt+1}/{retries})")
                time.sleep(2 * (attempt + 1))  # Exponential backoff
            except requests.exceptions.ConnectionError:
                print(f"  Connection error on {url} (attempt {attempt+1}/{retries})")
                time.sleep(3 * (attempt + 1))
            except Exception as e:
                print(f"  Error scraping {url}: {e}")
                return None
        
        print(f"  Giving up on {url} after {retries} retries")
        return None

    def crawl_arch_wiki(self, category_url=None, limit=10):
        """
        Crawls multiple Arch Wiki categories for maximum coverage.
        """
        categories = [
            "https://wiki.archlinux.org/title/Category:System_administration",
            "https://wiki.archlinux.org/title/Category:Boot_process",
            "https://wiki.archlinux.org/title/Category:Networking",
            "https://wiki.archlinux.org/title/Category:Security",
            "https://wiki.archlinux.org/title/Category:Kernel",
            "https://wiki.archlinux.org/title/Category:File_systems",
            "https://wiki.archlinux.org/title/Category:Package_management",
        ]
        
        if category_url:
            categories = [category_url]
        
        all_links = set()
        for cat_url in categories:
            try:
                print(f"Scanning category: {cat_url}")
                response = self.session.get(cat_url, timeout=15)
                soup = BeautifulSoup(response.content, 'lxml')
                
                for a in soup.select('#mw-pages a'):
                    href = a.get('href')
                    if href:
                        full_url = urljoin(cat_url, href)
                        all_links.add(full_url)
                        
                # Also get subcategory pages
                for a in soup.select('#mw-subcategories a'):
                    href = a.get('href')
                    if href and '/Category:' not in href:
                        full_url = urljoin(cat_url, href)
                        all_links.add(full_url)
                        
            except Exception as e:
                print(f"Error scanning category {cat_url}: {e}")
        
        print(f"Found {len(all_links)} unique pages across all categories")
        
        documents = []
        for i, link in enumerate(list(all_links)[:limit]):
            print(f"[{i+1}/{min(limit, len(all_links))}] Scraping {link}...")
            doc = self.scrape_url(link)
            if doc:
                documents.append(doc)
            time.sleep(1)  # Be polite
                
        print(f"Successfully scraped {len(documents)} wiki pages")
        return documents

if __name__ == "__main__":
    crawler = WebCrawler()
    doc = crawler.scrape_url("https://wiki.archlinux.org/title/Systemd")
    if doc:
        print(f"Scraped {doc['title']} ({len(doc['content'])} chars)")
