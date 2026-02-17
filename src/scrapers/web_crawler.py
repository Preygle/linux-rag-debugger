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

    def scrape_url(self, url):
        """
        Scrapes a single URL and extracts main content.
        This is a heuristic-based extraction.
        """
        print(f"Scraping {url}...")
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                print(f"Failed to fetch {url}: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Extract title
            title = soup.title.string if soup.title else "No Title"
            
            # Extract main content - simplistic approach
            # Arch Wiki uses 'mw-content-text'
            content_div = soup.find('div', {'id': 'mw-content-text'})
            if not content_div:
                content_div = soup.find('main')
            if not content_div:
                content_div = soup.body
                
            text = content_div.get_text(separator='\n')
            
            # cleanup empty lines
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            cleaned_text = '\n'.join(lines)
            
            return {
                'source': url,
                'title': title,
                'content': cleaned_text
            }
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def crawl_arch_wiki(self, category_url="https://wiki.archlinux.org/title/Category:System_administration", limit=10):
        """
        Simple crawler for Arch Wiki categories.
        """
        print(f"Crawling Arch Wiki category: {category_url}")
        try:
            response = self.session.get(category_url)
            soup = BeautifulSoup(response.content, 'lxml')
            
            links = []
            # Find pages in category
            for a in soup.select('#mw-pages a'):
                href = a.get('href')
                if href:
                    links.append(urljoin(category_url, href))
            
            documents = []
            for link in links[:limit]:
                doc = self.scrape_url(link)
                if doc:
                    documents.append(doc)
                time.sleep(1) # Be polite
                
            return documents
            
        except Exception as e:
            print(f"Error crawling Arch Wiki: {e}")
            return []

if __name__ == "__main__":
    crawler = WebCrawler()
    # Test single page
    doc = crawler.scrape_url("https://wiki.archlinux.org/title/Systemd")
    if doc:
        print(f"Scraped {doc['title']} ({len(doc['content'])} chars)")
