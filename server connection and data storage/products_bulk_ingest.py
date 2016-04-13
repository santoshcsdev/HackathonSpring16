import elasticsearch.helpers as es_help
from elasticsearch import Elasticsearch
import ast
import sys


def makeEsDoc(line):
	#print line
	src = ast.literal_eval(line)
	ret = {'_id' : src['asin'], 
		   '_index' : 'products', 
		   '_type' : 'product', 
		   '_source' : src}
	return ret

def main(fn):
	cnt = 0
	batch = []
	es = Elasticsearch()
	for line in open(fn):
		cnt += 1
		batch.append(makeEsDoc(line[:-1]))
		if len(batch) >= 50:
			stat = es_help.bulk(es, batch)
			print stat
			print "inserted", cnt
			print batch[0]['_id']
			batch = []

	stat = es_help.bulk(es, batch)
	print stat	
	print "inserted", cnt
	print batch[0]['_id']
	print "done"

main(sys.argv[1])
