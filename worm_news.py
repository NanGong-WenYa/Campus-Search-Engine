import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urljoin
import networkx as nx
import jieba

def fetch_page(url):
    """Fetch the page content from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_news(html):
    """Parse news content from HTML."""
    soup = BeautifulSoup(html, 'html.parser')

    # Extracting title
    title = soup.find('title').get_text(strip=True) if soup.find('title') else 'No title'

    # Extracting body text
    body = soup.find('td', id='txt')
    body_text = ''
    if body:
        body_text = '\n'.join(p.get_text(strip=True) for p in body.find_all('p'))

    # Perform jieba segmentation on body_text
    segmented_body = " ".join(jieba.cut(body_text)) if body_text else ""

    return {
        'title': title,
        'body_text': body_text,
        'segmented_body': segmented_body
    }

def is_valid_url(url):
    """Check if a URL is valid for crawling."""
    # URL must contain 'news' and should not point to non-HTML files
    return 'news.nankai' in url and not url.endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar','.mp4'))

def build_graph_and_extract_data(seed_url, max_pages=150000):
    """Build a directed graph of linked pages and extract news data."""
    graph = nx.DiGraph()
    visited = set()
    pages_crawled = 0
    extracted_data = []

    def crawl(url):
        nonlocal pages_crawled
        if pages_crawled >= max_pages or url in visited:
            return
        visited.add(url)
        if not is_valid_url(url):
            return
        pages_crawled += 1
        print(f"爬取了第 {pages_crawled} 页: {url}")
        html = fetch_page(url)
        if not html:
            return

        # Parse news content and store it
        news_info = parse_news(html)
        news_info['url'] = url  # Optionally include the URL for context
        extracted_data.append(news_info)

        # Add the page to the graph and continue crawling linked pages
        graph.add_node(url)
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a', href=True):
            full_url = urljoin(url, link['href'])
            if is_valid_url(full_url):  # Only crawl valid URLs
                graph.add_edge(url, full_url)
                crawl(full_url)

    crawl(seed_url)
    return graph, extracted_data

def save_to_csv(data, filename):
    """Save parsed data to a CSV file."""
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['title', 'body_text', 'segmented_body', 'url', 'pagerank'])
        writer.writeheader()
        writer.writerows(data)

def main():
    seed_url = "https://news.nankai.edu.cn/mtnk/system/2024/12/10/030064991.shtml"  # Replace with the seed URL

    # Step 1: Crawl and extract content
    graph, news_data = build_graph_and_extract_data(seed_url)

    # Step 2: Calculate PageRank
    pagerank = nx.pagerank(graph)

    # Step 3: Merge PageRank into data
    for item in news_data:
        item['pagerank'] = pagerank.get(item['url'], 0)

    # Step 4: Save to CSV
    save_to_csv(news_data, 'news_data.csv')
    print("Data saved to news_data.csv")

if __name__ == "__main__":
    main()
