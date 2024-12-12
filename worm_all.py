
import requests
from bs4 import BeautifulSoup
from bs4 import Comment

import csv
import networkx as nx
from urllib.parse import urlparse, urlunparse, urljoin
import re
import jieba  # 中文分词库

# 配置
START_URL = "https://www.nankai.edu.cn/"  # 起始 URL
MAX_PAGES =120  # 爬取的最大页面数量
OUTPUT_FILE = "output.csv"  # 输出文件名

# 初始化变量
visited_urls = set()
url_queue = [START_URL]
url_graph = nx.DiGraph()
data = []  # 存储结果数据
url_num=0


# 特殊字符替换函数
def clean_text(text):
    return re.sub(r'[\r\n\t\u00a0]', ' ', text).strip()

# 提取日期格式并转换为时间戳
def extract_timestamp(date_str):
    match = re.search(r'(\d{4}/\d{2}/\d{2})', date_str)
    if match:
        return int(datetime.strptime(match.group(1), "%Y/%m/%d").timestamp())
    
    return None

# 删除标点符号并排除网址内容的函数
def remove_punctuation_and_urls(text):
    # 移除标点符号
    text = re.sub(r'[^\w\s]', '', text)
    # 移除URL
    text = re.sub(r'(http[s]?://\S+|www\.\S+)', '', text)
    return text.strip()



# 对文本分词
def gettext(soup):
    # 要排除的 class 名
    excluded_classes = ["addr", "footer_icp", "head-links"]
    excluded_keywords = ["header","head", "menu","links","link","icon","page","footer"]  # 包含这些关键字的 class 也排除

    # 支持提取文本的标签
    tags = ['a', 'p', 'h1', 'h2', 'h3', 'br', 'b','div']
    text_list = []

    # # 过滤掉不需要的 div
    # for div in soup.find_all('div'):
    #     if not isinstance(div, Tag):  # 确保是有效的 Tag 对象
    #         continue
    #     if div is None:  # 检查是否为空
    #         continue
    #     div_classes = div.get("class", [])
    #     div_classes = [str(cls) for cls in div_classes]  # 转为字符串列表

        # # 如果符合排除条件，移除整个 div 节点
        # if any(cls in excluded_classes for cls in div_classes) or \
        #    any(keyword in " ".join(div_classes) for keyword in excluded_keywords):
        #     div.decompose()  # 从 DOM 树中移除该 div
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    for tag in tags:
        for element in soup.find_all(tag):
            # 获取该标签的 class 属性
            element_classes = element.get("class", [])
            # 转为字符串列表，方便过滤
            element_classes = [str(cls) for cls in element_classes]

            # 检查是否属于排除的 class
            if any(cls in excluded_classes for cls in element_classes) or \
               any(keyword in " ".join(element_classes) for keyword in excluded_keywords):
                continue  # 跳过这些标签

            # 提取文本
            text = element.get_text(strip=True)
            if text:  # 如果标签中有文本内容，加入列表
                text_list.append(text)

    # 将所有提取的文本拼接成一个字符串
    return " ".join(text_list)

def process_text(text):
    # 清理文本中的标点符号和URL
    text = remove_punctuation_and_urls(text)
    # 分词
    words = jieba.cut_for_search(text)
    # 使用集合去重，并返回分词结果
    unique_words = set(word.strip() for word in words if word.strip())
    return " ".join(unique_words)



def normalize_url(url):
    # 移除片段标识符
    parsed = urlparse(url)
    clean_url = urlunparse(parsed._replace(fragment=""))

    # 去掉查询参数
    clean_url = urlunparse(urlparse(clean_url)._replace(query=""))

    # 强制 HTTPS
    if clean_url.startswith("http://"):
        clean_url = clean_url.replace("http://", "https://")

    # 去掉末尾斜杠
    clean_url = clean_url.rstrip("/")

    # 转为小写
    clean_url = clean_url.lower()

    return clean_url

def crawl_page(url):
    global url_num
    try:
        response = requests.get(url, timeout=2)
        response.encoding = 'utf-8'

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            title = soup.title.string if soup.title else "No Title"
            text = gettext(soup)
            cleaned_text = process_text(text)

            links = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]

            # 过滤重复、外部链接及文件链接
            excluded_extensions = (
                ".doc", ".docx", ".pdf", ".xls", ".xlsx",
                ".ppt", ".pptx", ".zip", ".rar", ".jpg", ".png",
                ".mp3", ".mp4"
            )
            filtered_links = set()
            for link in links:
                norm_link = normalize_url(link)
                if (
                    norm_link.startswith(START_URL)
                    and norm_link not in visited_urls
                    and not any(norm_link.lower().endswith(ext) for ext in excluded_extensions)
                ):
                    filtered_links.add(norm_link)

            for link in filtered_links:
                url_graph.add_edge(url, link)

            url_num += 1
            print(f"Crawling ({url_num}): {url}")
            return title, cleaned_text, list(filtered_links)
        else:
            print(f"Failed to fetch {url}: {response.status_code}")
            return None, None, []
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None, None, []


# 主程序
while url_queue and len(visited_urls) < MAX_PAGES:
    current_url = url_queue.pop(0)
    if current_url in visited_urls:
        continue


    title, text, links = crawl_page(current_url)
    

    if title and text:
        visited_urls.add(current_url)
        url_queue.extend(links)
        data.append((current_url, title, text))

# 计算 PageRank
pagerank_scores = nx.pagerank(url_graph)

# 将结果保存到 CSV 文件
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["URL", "Title", "Text", "PageRank"])
    for url, title, text in data:
        pagerank = pagerank_scores.get(url, 0)
        writer.writerow([url, title, text, pagerank])

print(f"Data saved to {OUTPUT_FILE}")

