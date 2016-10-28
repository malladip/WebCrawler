__author__ = 'abhiram'

from pprint import pprint
from elasticsearch import Elasticsearch
import json
import urllib
es = Elasticsearch("localhost:9200",timeout = 600,max_retries = 1,revival_delay = 0)

def write_to_es(url, text, title, in_links, out_links, raw_html,http_header):
    if es.exists(index="test_index_skpr", doc_type="document", id=url):
        doc = es.get(index="test_index_skpr", doc_type="document", id=url)
        new_in_links = set(in_links) | set(doc['_source']['in_links'])
        new_out_links = set(out_links) | set(doc['_source']['out_links'])
        es.update(index="test_index_skpr",
                  doc_type="document",
                  id=url,
                  body={"doc": {"in_links": list(new_in_links), "out_links": list(new_out_links)}})
    else:
        contents = {'docno':url,'HTTPheader':http_header,'title':title, 'text': text,'html_Source':raw_html, 'in_links': list(in_links), 'out_links': list(out_links),'author':'ravi'}
        es.index(index="test_index_skpr", doc_type="document", id=url, body=contents)

count= 0
def merge(docs,filenum):
    global count
    for key in docs:
        count = count + 1
        print(count)
        print(key)
        doc = docs[key]
        try:
            in_out = in_out_links[key]
            write_to_es(key, doc[1], doc[0], in_out[0] ,in_out[1],doc[2],doc[3])
        except KeyError:
            write_to_es(key, doc[1], doc[0], [] ,[],doc[2],doc[3])

        # in_out = in_out_links[key]
        # write_to_es(key, doc[1], doc[0], in_out[0] ,in_out[1],doc[2],doc[3])

    with open("file_num.txt", "w+") as fc:
        json.dump({"num":filenum,"count":count},fc)

in_out_links = {}
with open("ravi_in_out_links.txt", "r") as c:
    in_out_links = json.load(c)


filenum = 0
import glob
files = glob.glob("data/*")

file_num = {}
with open("file_num.txt", "r") as c:
    file_num = json.load(c)

count = file_num["count"]
for filename in files:
    filenum = filenum +1
    with open(filename,'r+') as fc:
        if file_num["num"] > filenum:
            print(filenum)
            continue
        print(filenum)
        docs = json.load(fc)
        merge(docs,filenum)
