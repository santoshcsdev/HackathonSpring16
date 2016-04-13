from flask import Flask, request, Response
from flask import render_template
import logging
import json
from logging.handlers import RotatingFileHandler
from elasticsearch import Elasticsearch

# initialize logging
handler = RotatingFileHandler(
    'server.log',
    maxBytes=1000000,
    backupCount=100)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)



app = Flask(__name__)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)
LOG = app.logger
LOG.info("log started")
es = Elasticsearch()

@app.route('/products')
def product():
    args = request.args
    uquery = args.get('query')
    res = es.search(
    	index="products", doc_type="product",
    	body={"query": {"match": {"title": uquery}}})
    return Response(json.dumps(res['hits']), mimetype='application/json')


@app.route('/reviews')
def reviews():
    args = request.args
    product_id = args.get('product')
    res = es.search(
    	index="reviews", doc_type="review",
    	body={"query": {"match": {"asin": product_id}}})
    return Response(json.dumps(res['hits']), mimetype='application/json')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8586)
