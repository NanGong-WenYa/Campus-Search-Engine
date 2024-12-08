import os
import pandas as pd
from datetime import datetime
from utils import news_crawler
from utils import preprocessing
nankai_template_url = 'https://news.nankai.edu.cn/'
project_path = os.path.dirname(os.path.realpath(__file__))  # 获取项目路径
news_path = os.path.join(project_path, 'news')  # 新闻数据存放目录路径
if not os.path.exists(news_path):  # 创建news文件夹
    os.mkdir(news_path)

# 爬取南开大学新闻
nankai_news_df = news_crawler.get_nankai_latest_news(nankai_template_url, total_news=150000, show_content=True)

# 将南开大学新闻保存为 CSV 文件（已经在爬取过程中进行多次保存）
# 这里不需要再次保存，因为每1000条已经保存过了
# news_crawler.save_news(nankai_news_df, os.path.join(news_path, 'nankai_latest_news.csv'))

# 其他操作：数据清洗
news_df = preprocessing.data_filter(nankai_news_df)
news_df['content'] = news_df['content'].map(lambda x: preprocessing.clean_content(x))

# 最终保存
news_crawler.save_news(news_df, os.path.join(news_path, 'nankai_latest_news.csv'))
