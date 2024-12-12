import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urljoin
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

def parse_xs_page(html):
    """Parse title and body content from a 校史网 page."""
    soup = BeautifulSoup(html, 'html.parser')

    # Extract title
    title = soup.find('title').get_text(strip=True) if soup.find('title') else 'No title'

    # Extract body text from <p> tags
    body_text = '\n'.join(p.get_text(strip=True) for p in soup.find_all('p'))

    # Perform jieba segmentation
    segmented_body = " ".join(jieba.cut(body_text)) if body_text else ""

    return {
        'title': title,
        'body_text': body_text,
        'segmented_body': segmented_body
    }

def is_valid_xs_url(url):
    """Check if a URL is valid for crawling (校史类页面)."""
    return 'xs.nankai' in url and not url.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar','.mp4','jpg','png'))

def crawl_xs_website(seed_url, max_pages=20):
    """Crawl 校史网站 starting from the seed URL."""
    visited = set()
    queue = [seed_url]
    data = []
    page_count = 0

    while queue and page_count < max_pages:
        url = queue.pop(0)
        if url in visited or not is_valid_xs_url(url):
            continue

        visited.add(url)
        print(f"Crawling page {page_count + 1}: {url}")
        html = fetch_page(url)
        if not html:
            continue

        # Parse the current page
        page_data = parse_xs_page(html)
        if page_data:
            data.append(page_data)
            page_count += 1

        # Extract links for the next pages
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a', href=True):
            full_url = urljoin(url, link['href'])
            if is_valid_xs_url(full_url) and full_url not in visited:
                queue.append(full_url)

    return data

def append_to_csv(data, filename):
    """Append parsed 校史 data to an existing CSV file."""
    with open(filename, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['title', 'body_text', 'segmented_body'])
        writer.writerows(data)

def main():
    seed_url = "https://xs.nankai.edu.cn"  # Replace with the 校史网 seed URL
    max_pages = 20
    output_file = 'news_data.csv'  # Append 校史数据到 news_data.csv

    # Start crawling
    print(f"Starting crawl from {seed_url} with a limit of {max_pages} pages...")
    crawled_data = crawl_xs_website(seed_url, max_pages)

    # Append results to CSV
    append_to_csv(crawled_data, output_file)
    print(f"Crawl completed. Data appended to {output_file}")

if __name__ == "__main__":
    main()
