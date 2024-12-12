import csv
from elasticsearch import Elasticsearch, helpers
import json

# 配置
ES_HOST = "http://localhost:9200"  # Elasticsearch 主机地址
INDEX_NAME = "nankai_index"  # 要创建的索引名称
CSV_FILE = "news_data.csv"  # 爬取后生成的 CSV 文件名

# 连接 Elasticsearch
es = Elasticsearch(ES_HOST)

# 创建索引的映射（可选，定制字段类型）
def create_index():
    # 创建索引的映射
    mappings = {
        "mappings": {
            "properties": {
                "URL": {"type": "text"},
                "Title": {"type": "text"},
                "Text": {"type": "text"},
                "PageRank": {"type": "float"}
            }
        }
    }

    # 如果索引不存在，则创建
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=mappings)
        print(f"Index '{INDEX_NAME}' created successfully!")
    else:
        print(f"Index '{INDEX_NAME}' already exists.")

# 读取 CSV 文件并生成文档列表
def generate_documents():
    documents = []
    with open(CSV_FILE, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                document = {
                    "_index": INDEX_NAME,
                    "_source": {
                        "URL": row["url"],
                        "Title": row["title"],
                        "Text": row["segmented_body"],
                        "PageRank": float(row["pagerank"])
                    }
                }
                documents.append(document)
            except KeyError as e:
                print(f"Missing field in row: {e}, skipping row...")
    return documents

# 批量插入文档到 Elasticsearch
def index_documents(documents):
    try:
        helpers.bulk(es, documents)
        print(f"Successfully indexed {len(documents)} documents.")
    except Exception as e:
        print(f"Error while indexing: {e}")

# 查询示例：搜索所有文档
def search_documents():
    query = {
        "query": {
            "match_all": {}
        }
    }
    try:
        response = es.search(index=INDEX_NAME, body=query)
        print(f"Found {response['hits']['total']['value']} documents.")
        for hit in response['hits']['hits']:
            print(f"Title: {hit['_source']['Title']}, URL: {hit['_source']['URL']}")
    except Exception as e:
        print(f"Error while searching: {e}")

if __name__ == "__main__":
    # 创建索引
    create_index()

    # 生成文档列表
    documents = generate_documents()

    # 批量插入文档
    if documents:
        index_documents(documents)

    # 查询索引中的文档
    search_documents()
