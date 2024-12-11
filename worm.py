import csv
import jieba
import re
import networkx as nx
from selenium import webdriver

import lxml.html as lh
from bs4 import BeautifulSoup
import urllib.request
import time

from urllib.parse import urljoin  # 在模块顶部增加这个导入

# 初始化全局变量
urllist = dict()
pageranks = dict()
g = nx.DiGraph()

# 写入表头（确保表头只写一次）
with open('urls.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['url'])  # 写入表头



def getPageUrl(url):
    try:
        html_data = urllib.request.urlopen(url, timeout=10)
        html = html_data.read().decode("utf-8")
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        urllist[url] = 2  # 标记为错误
        return

    if not g.has_node(url):
        g.add_node(url)

    soup = BeautifulSoup(html, features='html.parser')
    tags = soup.find_all('a')

    with open('urls.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for tag in tags:
            href = tag.get('href')
            if href is None or href == '' or href.startswith('#'):
                continue
            if href == "/" or "javascript:" in href:
                continue

            # 处理相对路径，使用 urljoin 进行标准化
            href = urljoin(url, href)

            # 保存到 urllist 和文件中
            if href not in urllist and "nankai.edu.cn" in href:
                urllist[href] = 0
                writer.writerow([href])  # 写入 URL 到文件
            if not g.has_node(href):
                g.add_node(href)
            if not g.has_edge(url, href):
                g.add_edge(url, href)

    urllist[url] = 1

# def getPageUrl(url):
#     try:
#         html_data = urllib.request.urlopen(url, timeout=10)
#         html = html_data.read().decode("utf-8")
#     except Exception as e:
#         print(f"Error fetching URL {url}: {e}")
#         urllist[url] = 2  # 标记为错误
#         return

#     if not g.has_node(url):
#         g.add_node(url)

#     soup = BeautifulSoup(html, features='html.parser')
#     tags = soup.find_all('a')

#     with open('urls.csv', 'a', newline='', encoding='utf-8') as f:
#         writer = csv.writer(f)
#         for tag in tags:
#             href = tag.get('href')
#             if href is None or href == '' or href.startswith('#'):
#                 continue
#             if href == "/" or "javascript:" in href:
#                 continue

#             # 处理相对路径
#             if "http" not in href:
#                 if href.startswith('//'):
#                     href = "http:" + href
#                 elif href.startswith('/'):
#                     href = url.split('/')[0] + '//' + url.split('/')[2] + href
#                 else:
#                     href = url.split('/')[0] + '//' + url.split('/')[2] + '/' + href

#             # 保存到 urllist 和文件中
#             if href not in urllist and "nankai.edu.cn" in href:
#                 urllist[href] = 0
#                 writer.writerow([href])  # 写入 URL 到文件
#             if not g.has_node(href):
#                 g.add_node(href)
#             if not g.has_edge(url, href):
#                 g.add_edge(url, href)

#     urllist[url] = 1


def getAllUrl(rootUrl, urllimit=150):
    getPageUrl(rootUrl)
    urllist[rootUrl] = 1

    while 0 in urllist.values():
        for url in list(urllist.keys()):
            if urllist[url] == 2:
                continue
            if urllist[url] == 0:
                getPageUrl(url)
                time.sleep(1)  # 延时 1 秒，避免爬取过快被封禁
            if len(urllist) >= urllimit:
                return


def PageRank():
    # 计算图的 PageRank
    return nx.algorithms.link_analysis.pagerank(g)\
    

import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import jieba
import jieba.analyse
from urllib.parse import urljoin

# 读取URL列表
urls_csv = "urls.csv"
output_csv = "output.csv"

# DataFrame初始化
columns = ["url", "title", "description", "date_timestamp", "content", "editor"]
df = pd.DataFrame(columns=columns)

# 存储页面中包含的URL的字典
url_links_dict = {}

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

# 处理内容和编辑者分离
def split_content(content):
    editor_info = ""
    if "编辑" in content:
        content_parts = content.split("编辑", 1)
        content = content_parts[0].strip()
        editor_info = content_parts[1].strip()
    return content, editor_info

# 对文本分词
jieba.setLogLevel(jieba.logging.INFO)
def process_text(text):
    # 清理文本中的标点符号和URL
    text = remove_punctuation_and_urls(text)
    # 分词
    words = jieba.cut_for_search(text)
    # 返回分词结果
    return " ".join([word.strip() for word in words if word.strip()])


root_url = "https://www.nankai.edu.cn"
getAllUrl(root_url)

# 请求和解析HTML
for index, row in pd.read_csv(urls_csv).iterrows():
    url = row['url']
    try:
        response = requests.get(url, timeout=10)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, 'html.parser')

        # 提取title
        title = soup.title.string if soup.title else ""
        title = title.replace("/", "_") if title else ""

        # 提取description
        description_meta = soup.find('meta', attrs={'name': 'description'})
        description = description_meta['content'] if description_meta else ""

        # 提取正文内容
        content = " ".join([p.get_text() for p in soup.find_all('p')])
        content = clean_text(content)

        # 分离内容和编辑信息
        content, editor = split_content(content)

        # 提取发布时间
        date_meta = soup.find('meta', attrs={'name': 'date'})
        date_str = date_meta['content'] if date_meta else ""
        date_timestamp = extract_timestamp(date_str)

        # 分词处理
        title = process_text(title)
        description = process_text(description)
        content = process_text(content)

        # 提取页面中包含的URL
        links = [urljoin(url, a['href']) for a in soup.find_all('a', href=True)]
        url_links_dict[url] = links

        # 添加到DataFrame
        df = pd.concat([
            df,
            pd.DataFrame({
                "url": [url],
                "title": [title],
                "description": [description],
                "date_timestamp": [date_timestamp],
                "content": [content],
                "editor": [editor]
            })
        ], ignore_index=True)

    except Exception as e:
        print(f"Error processing URL {url}: {e}")

# 保存结果
df.to_csv(output_csv, index=False)

# 保存URL链接字典
import json
with open("url_links.json", "w", encoding="utf-8") as f:
    json.dump(url_links_dict, f, ensure_ascii=False, indent=4)



# import re
# import jieba
# from selenium import webdriver
# from lxml import etree

# def get_text(url):
#     options = webdriver.ChromeOptions()
#     options.add_argument('--headless')
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
#     driver = webdriver.Chrome(options=options)

#     try:
#         driver.get(url)
#         html_source = driver.page_source
#         name = driver.title
#         driver.quit()
#     except Exception as e:
#         print(f"Error accessing URL {url}: {e}")
#         driver.quit()
#         return '', ''

#     try:
#         # 使用 lxml 解析 HTML
#         html = lh.fromstring(html_source)
        
#         # 使用 XPath 提取目标文本
#         texts = html.xpath("//p//text()|//h1//text()|//h2//text()|//h3//text()|//br//text()|//b//text()")
        
#         # 使用正则表达式清理前后空白字符和换行符
#         cleaner = re.compile(r"^\s+|\s+$|\n")
#         cleaned_texts = [cleaner.sub("", text) for text in texts if text.strip()]  # 去除空内容
        
#         # 将清理后的文本拼接成单个字符串
#         filtered_text = ' '.join(cleaned_texts)
        
#         # 使用 jieba 分词
#         segmented_text = " ".join(jieba.cut(filtered_text))
#     except Exception as e:
#         print(f"Error processing HTML content: {e}")
#         return name, ''

#     return name, segmented_text



# def save_to_final_csv(url, title, segmented_text, pagerank):
#     with open('combined_result.csv', 'a', newline='', encoding='utf-8') as f:
#         writer = csv.writer(f)
#         writer.writerow([url, title, segmented_text, pagerank])


# def process_urls_from_csv():
#     pageranks = PageRank()

#     with open('urls.csv', 'r', newline='', encoding='utf-8') as f:
#         reader = csv.reader(f)
#         rows = list(reader)

#         if not rows:
#             print("urls.csv 文件为空")
#             return
#         if len(rows) == 1:
#             print("urls.csv 只有表头，没有数据行")
#             return

#         for row in rows[1:]:
#             url = row[0]
#             title, segmented_text = get_text(url)
#             pagerank = pageranks.get(url, 0)
#             save_to_final_csv(url, title, segmented_text, pagerank)


# # 主程序入口
# root_url = "https://www.nankai.edu.cn"
# getAllUrl(root_url)
# process_urls_from_csv()
