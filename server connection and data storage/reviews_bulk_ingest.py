import elasticsearch.helpers as es_help
from elasticsearch import Elasticsearch
import json
import sys


def makeEsDoc(line):
	#print line
	src = json.loads(line)
	ret = {'_id' : src['asin'] + '.' + src['reviewerID'], 
		   '_index' : 'reviews', 
		   '_type' : 'review', 
		   '_source' : src}
	return ret

def main(fn):
	cnt = 0
	batch = []
	es = Elasticsearch()
	for line in open(fn):
		cnt += 1
		batch.append(makeEsDoc(line[:-1]))
		if len(batch) >= 1000:
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
