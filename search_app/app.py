from flask import Flask, request, render_template
from elasticsearch import Elasticsearch

app = Flask(__name__)

# Elasticsearch 配置
ES_HOST = "http://localhost:9200"
INDEX_NAME = "nankai_index"

# 初始化 Elasticsearch 客户端
es = Elasticsearch(ES_HOST)

@app.route("/", methods=["GET", "POST"])
def index():
    query = request.form.get("query", "")  # 获取搜索关键词
    results = []

    if query:  # 如果有搜索请求
        # 构建搜索请求
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["Title", "Text"]  # 搜索标题和正文
                }
            }
        }
        try:
            # 执行搜索
            response = es.search(index=INDEX_NAME, body=search_body)
            # 提取搜索结果
            results = [
                {
                    "title": hit["_source"]["Title"],
                    "url": hit["_source"]["URL"],
                    "pagerank": hit["_source"].get("PageRank", 0.0),
                }
                for hit in response["hits"]["hits"]
            ]
        except Exception as e:
            print(f"Error during search: {e}")

    return render_template("index.html", query=query, results=results)


if __name__ == "__main__":
    app.run(debug=True)
