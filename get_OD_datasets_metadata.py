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
import pickle

from elasticsearch import Elasticsearch

from gensim.models import Phrases


API = 'http://portalwatch.ai.wu.ac.at/csvsearch/es/portals/_search?q=%s'
PORTALS = ['http://data.hawaii.gov', 'http://data.gov.uk/']
STOPWORDS = ['and', 'by', 's', 'http', 'in']
INDEX = 'open_data'
WORD_DICT = 'wordfreq_log.pickle'


def get_from_ES():
    es = Elasticsearch()
    return es.search(index=INDEX, doc_type='dataset', explain=False)['hits']['hits']


def tokenize(string, stopwords=STOPWORDS):
    """Convert string to lowercase and split into words (ignoring
    punctuation), returning list of words.
    """
    # remove text in brackets
    string = re.sub(r'\(.*\)', '', string)
    return [token for token in re.findall(r'\w+', string.lower()) if token not in stopwords]


def index_datasets(index_name=INDEX):
    es = Elasticsearch()
    # get data from the API
    for portal in PORTALS:
        query = 'catalog.url:%22' + portal + '%22'
        resp = requests.get(API % (query))
        print (resp)
        results = json.loads(resp.text)['hits']['hits']
        for dataset in results:
            doc = dataset['_source']
            # store in ES
            es.index(index=index_name, doc_type='dataset',
                     body=doc)


def analyze_collection():
    # load word frequencies dictionary
    word_freqs = pickle.load(open( WORD_DICT, "rb" ))

    docs = []
    # count words
    counter = Counter()
    portals = Counter()

    results = get_from_ES()

    for dataset in results:
        doc = dataset['_source']
        # print doc

        # shared meta attributes indicating information source
        url = doc['catalog']['url']
        url_tokens = tokenize(url, stopwords=STOPWORDS)

        # print doc['publisher']['name']
        
        # shared content attributes
        # if 'keywords' in doc.keys():
        #     print doc['keywords']

        # unique content attributes
        name = doc['name']

        # make sure to account only single occurance of token per document
        # remove stopwords
        unique_tokens_url = [token for token in set(url_tokens)]
        # filter terms already represented in other dimension
        tokens = [token for token in tokenize(name) if token not in unique_tokens_url]
        unique_tokens = [token for token in set(tokens)]
        
        print (name)
        print (tokens)
        print (unique_tokens)

        # collect
        docs.append(tokens)
        counter.update(unique_tokens)
        portals.update(unique_tokens_url)

        # print doc['description'].replace('\n', ' ')

        print ('\n')
    ndocs = len(docs)
    print (ndocs, 'docs')
    # collection counter 
    # print counter
    # print '\n'

    # calculate term frequencies in the collection
    collection_frequencies = Counter()

    # for word, count in counter.iteritems():
    #     collection_frequencies[word] = count/float(ndocs)

    # count ngrams
    ngrams = form_nrgams(docs)
    for doc in ngrams:
        for ngram in doc:
            # filter out unigrams
            if len(tokenize(ngram)) > 1:
                # counter[ngram] += 1
                collection_frequencies[ngram] += 1  # /float(ndocs)

    # show results
    print ('Portals (urls) dimension:', portals.most_common(20))
    # print 'Topics (dataset names) dimension:', counter.most_common(20)
    print (collection_frequencies.most_common(10))


def form_nrgams(docs):
    ngrams = Phrases(docs, min_count=1, threshold=2, delimiter=b' ')
    # recursion to form max number of ngrams with the specified threshold
    # if list(ngrams[docs]) != list(docs):
        # return form_nrgams(ngrams[docs])
    return list(ngrams[docs])

    # trigrams = Phrases(bigrams[docs], min_count=1, threshold=2, delimiter=' ')
    # return list(trigrams[bigrams[docs]])

if __name__ == '__main__':
    analyze_collection()
    # index_datasets()
