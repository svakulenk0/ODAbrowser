'''
svakulenko
7 Aug 2017

Getting metadata of Open Datasets from Portal Watch ES index.
Sample query: http://portalwatch.ai.wu.ac.at/csvsearch/es/portals/_search?q=catalog.url:%22http://data.gov.uk/%22
'''
import requests
import json
import re
from collections import Counter

from elasticsearch import Elasticsearch

from gensim.models import Phrases


API = 'http://portalwatch.ai.wu.ac.at/csvsearch/es/portals/_search?q=%s'
PORTALS = ['http://data.hawaii.gov', 'http://data.gov.uk/']
STOPWORDS = ['and', 'by', 's']
INDEX = 'open_data'


def get_from_ES():
    es = Elasticsearch()
    return es.search(index=INDEX, doc_type='dataset', explain=False)['hits']['hits']


def tokenize(string):
    """Convert string to lowercase and split into words (ignoring
    punctuation), returning list of words.
    """
    return re.findall(r'\w+', string.lower())


def index_datasets(index_name=INDEX):
    es = Elasticsearch()
    # get data from the API
    for portal in PORTALS:
        query = 'catalog.url:%22' + portal + '%22'
        resp = requests.get(API % (query))
        print resp
        results = json.loads(resp.text)['hits']['hits']
        for dataset in results:
            doc = dataset['_source']
            # store in ES
            es.index(index=index_name, doc_type='dataset',
                     body=doc)


def analyze_collection():
    docs = []
    # count words
    counter = Counter()

    results = get_from_ES()

    for dataset in results:
        doc = dataset['_source']
        # print doc

        # shared meta attributes indicating information source
        print doc['catalog']['url']
        # print doc['publisher']['name']
        
        # shared content attributes
        # if 'keywords' in doc.keys():
        #     print doc['keywords']

        # unique content attributes
        name = doc['name']

        tokens = tokenize(name)

        # make sure to account only single occurance of token per document
        # remove stopwords
        unique_tokens = [token for token in set(tokens) if token not in STOPWORDS]
        
        print name
        print tokens
        print unique_tokens

        # collect
        docs.append(tokens)
        counter.update(unique_tokens)

        # print doc['description'].replace('\n', ' ')

        print '\n'
    ndocs = len(docs)
    print ndocs, 'docs'
    # collection counter 
    # print counter
    # print '\n'

    # calculate term frequencies in the collection
    collection_frequencies = Counter()
    for word, count in counter.iteritems():
        collection_frequencies[word] = count/float(ndocs)

    # count ngrams
    ngrams = form_nrgams(docs)
    for doc in ngrams:
        for ngram in doc:
            # filter stopwords
            if ngram not in STOPWORDS:
                # filter out unigrams
                if len(tokenize(ngram)) > 1:
                    counter[ngram] += 1
                    collection_frequencies[ngram] += 1/float(ndocs)
    # show results
    print counter.most_common(20)
    # print collection_frequencies.most_common(10)


def form_nrgams(docs):
    ngrams = Phrases(docs, min_count=1, threshold=2, delimiter=' ')
    # recursion to form max number of ngrams with the specified threshold
    if list(ngrams[docs]) != list(docs):
        return form_nrgams(ngrams[docs])
    return list(ngrams[docs])

    # trigrams = Phrases(bigrams[docs], min_count=1, threshold=2, delimiter=' ')
    # return list(trigrams[bigrams[docs]])

if __name__ == '__main__':
    analyze_collection()
    # index_datasets()
