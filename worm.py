import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import jieba
import re
import csv
import networkx as nx

def remove_copyright(text):
    """Remove copyright information in xs.nankai pages."""
    copyright_text = "Copyright © 2020 南开大学 津教备0061号   津ICP备12003308号-1津公网安备12010402000967号"
    return text.replace(copyright_text, "")


def remove_punctuation(text):
    """Remove punctuation using regular expressions."""
    return re.sub(r'[^\w\s]', '', text)


async def fetch_page(url, session):
    """Fetch the page content asynchronously."""
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"Failed to fetch {url}, status: {response.status}")
                return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def parse_page(html, url):
    """Parse title and body content based on the URL type."""
    soup = BeautifulSoup(html, 'html.parser')

    # Extract title
    title = soup.find('title').get_text(strip=True) if soup.find('title') else 'No title'

    # Extract body text based on URL
    if 'news.nankai' in url:
        body = soup.find('td', id='txt')
        body_text = '\n'.join(p.get_text(strip=True) for p in body.find_all('p')) if body else ""
    elif 'xs.nankai' in url:
        body_text = '\n'.join(p.get_text(strip=True) for p in soup.find_all('p'))
        body_text = remove_copyright(body_text)  # Remove copyright text for xs.nankai pages
    else:
        body_text = ""

    # Perform jieba segmentation
    segmented_body = jieba.lcut_for_search(body_text) if body_text else []
    clean_segmented_body = " ".join(remove_punctuation(word) for word in segmented_body)

    return {'title': title, 'segmented_body': clean_segmented_body}


def is_valid_url(url):
    """Check if a URL is valid for crawling."""
    return (
        ('news.nankai' in url or 'xs.nankai' in url) and
        not url.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.mp4', 'jpg', 'png'))
    )


async def crawl_website(seed_url, max_pages=50):
    """Crawl both News and 校史 pages asynchronously."""
    visited = set()
    queue = [seed_url]
    data = []
    graph = nx.DiGraph()
    page_count = 0

    async with aiohttp.ClientSession() as session:
        while queue and page_count < max_pages:
            url = queue.pop(0)
            if url in visited or not is_valid_url(url):
                continue

            visited.add(url)
            print(f"Crawling page {page_count + 1}: {url}")
            html = await fetch_page(url, session)
            if not html:
                continue

            # Parse the current page
            page_data = parse_page(html, url)
            if page_data:
                page_data['url'] = url  # Add URL for PageRank mapping
                data.append(page_data)
                graph.add_node(url)

            # Extract links for the next pages
            soup = BeautifulSoup(html, 'html.parser')
            for link in soup.find_all('a', href=True):
                full_url = urljoin(url, link['href'])
                if is_valid_url(full_url) and full_url not in visited:
                    queue.append(full_url)
                    graph.add_edge(url, full_url)

            page_count += 1

    return data, graph


def save_to_csv(data, filename):
    """Save parsed data to a CSV file."""
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'title', 'segmented_body', 'pagerank'])
        writer.writeheader()
        writer.writerows(data)


async def main():
    # Start crawling from a single seed URL
    seed_url = "https://news.nankai.edu.cn/"
    max_pages = 100500

    print("Starting to crawl websites containing 'news' or 'xs.nankai'...")
    data, graph = await crawl_website(seed_url, max_pages)

    # Calculate PageRank
    pagerank = nx.pagerank(graph)

    # Add PageRank to data
    for item in data:
        item['pagerank'] = pagerank.get(item['url'], 0)

    # Save to CSV
    save_to_csv(data, 'news_data.csv')
    print("All data saved to news_data.csv")


if __name__ == "__main__":
    asyncio.run(main())
