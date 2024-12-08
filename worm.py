import urllib.request
import networkx as nx
from bs4 import BeautifulSoup
import csv
import time  # 引入time模块来实现延时


import sys

class TailCallException(BaseException):
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs

def tail_call_optimized(func):
    def _wrapper(*args, **kwargs):
        f = sys._getframe()
        if f.f_back and f.f_back.f_back and f.f_code == f.f_back.f_back.f_code:
            raise TailCallException(args, kwargs)

        else:
            while True:
                try:
                    return func(*args, **kwargs)
                except TailCallException as e:
                    args = e.args
                    kwargs = e.kwargs
    return _wrapper


# 图的初始化

urllist = dict()
pageranks = dict()
lab404 = 0
f = open('urls.csv', 'w', newline='', encoding='utf-8')  # 保证文件全局存在
writer = csv.writer(f)
writer.writerow(['url'])  # 写入表头


def getPageUrl(url):
    try:
        html = urllib.request.urlopen(url, timeout=1)
        html = html.read()
        html = html.decode("utf-8")
    except:
        urllist[url] = 2  # 标记为错误
        return

    
    soup = BeautifulSoup(html, features='html.parser')
    tags = soup.find_all('a')
    count = 0
    for tag in tags:
        href = tag.get('href')
        if href is None or href == '':
            continue
        if href[0] == '#':  # 若是以"#"开头则不用处理
            continue
        if href == "/" or href == "#" or "javascript:" in href:  # 跳过无效链接
            continue

        # 拼接相对路径为绝对路径
        if "http" not in href:
            if href[0] == '/' and href[1] == '/':
                href = "http:" + href
            elif href[0] == '/':
                href = url.split('/')[0] + '//' + url.split('/')[2] + href
            else:
                href = url.split('/')[0] + '//' + url.split('/')[2] + '/' + href

        count += 1

        # 将链接存储到 urllist 中
        if href not in urllist and "nankai.edu.cn" in href and '/c/' not in href:
            urllist[href] = 0
            writer.writerow([href])

    urllist[url] = 1  # 完成当前 URL 的处理

# 获取所有 URL

@tail_call_optimized
def getAllUrl(rootUrl, urllimit=150000):
    getPageUrl(rootUrl)  # 从根 URL 开始获取
    urllist[rootUrl] = 1  # 将根 URL 设置为已处理

    # 爬取直到达到 URL 限制
    while 0 in list(urllist.values()):  # 只处理未完成的 URL
        for url in list(urllist):  # 使用list(urllist)确保每次都遍历当前的URL
            if urllist[url] == 2:
                continue  # 如果当前 URL 已经处理过（失败），跳过
            if urllist[url] == 0:  # 如果是待处理的 URL
                getAllUrl(url)
                # 每 50 个 URL 保存一次，并输出提醒
                if len(urllist) % 50 == 0:
                    print(f"已保存 {len(urllist)} 个 URL")

                # 添加延时，避免被识别为爬虫
                time.sleep(1)  # 暂停 1 秒

            # 如果达到指定的 URL 数量限制，则退出
            if len(urllist) >= urllimit:
                break

        if len(urllist) >= urllimit:  # 达到 URL 限制，退出
            break
    


# 调用函数，爬取 URL
root_url = "https://www.nankai.edu.cn"
getAllUrl(root_url)
