from mongoengine import *
import pandas as pd
import pymongo
from odo import odo
from pprint import  pprint
import multiprocessing as mp
import click

from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.test_database

from nltk.tokenize import word_tokenize
from nltk.util import ngrams

result =  db['result']




def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]


def clean(text):
    try:
        text = str(text)
        a = text .replace('[','').replace(']','').replace('"','')
        return a
    except:
        return 'error'



def get_ngrams(text, n=1 ):
    n_grams = ngrams(word_tokenize(text), n)
    #return [' '.join(grams) for grams in n_grams]
    return  [' '.join(grams) for grams in n_grams]




def import_data(name):
    print('reading file......')
    df = pd.read_excel('{}'.format(name), encoding="ISO-8859-1")
    print('start processing file')
    df.columns = df.columns.str.replace(".", "_")
    cols = [col for col in df.columns if
            col in ['Keyword', 'Campaign', 'Ad group', 'Status', 'Clicks', 'Impressions', 'Cost', 'Conversions',
                    'Match type']]
    df = df[cols]
    print('clean up keywords')
    df['Keyword'] = df['Keyword'].map(clean)
    print('Delete All Records first')
    db.nword2.remove({})
    print('Clear  All Saved Records first')
    db.result.remove({})
    ng = odo(df, db.nword2)
    return df






ngword = []


def get_word(n,df ):
    global ngword
    ke = df['Keyword'].tolist()
    for x in ke:
        ngword.extend(get_ngrams(x,n))
    return list(set(ngword))

#pprint(word.find_one())






def get_mongo_data(word):

    x = db.nword2.find({"Keyword": {'$regex': ".*{}.*".format(word)}})

    click = []
    imp = []
    conv = []
    cost = []

    for cur in x:
        #pprint(cur)
        click.append(cur['Clicks'])
        imp.append(cur['Impressions'])
        conv.append(cur['Conversions'])
        cost.append(cur['Cost'])

    #pprint(x.count())
    # pprint(x['Avg_ CPC'].sum())
    # pprint(sum(cl))

    # click = [ int(y['Clicks']) for y in  x ]
    # cost  = [ int(c['Cost']) for c in  x]
    # imp   = [ int(im['Impressions']) for im  in  x]
    # conv  = [ int(con['Conversions']) for con in  x]


    #pprint('click : {}'.format(sum(click)))
    # pprint(click)
    #pprint('Cost : {} '.format(round(sum(cost), 2)))
    #pprint('imp : {} '.format(sum(imp)))
    #pprint('conv : {}'.format(sum(conv)))
    #print('keyword : {}'.format(word))

    return db.result.insert_one({
        'keyword':word,
        'click':sum(click),
        'imp': sum(imp),
        'cost' : round(sum(cost), 2),
        'conv': sum(conv),
        'CTR': sum(click)/sum(imp) if sum(imp) else 0,
        'CPC': round(sum(cost), 2)/sum(click) if sum(click) else 0,
        'conv_rate': sum(conv)/sum(click) if sum(click) else 0
    })






@click.command()
@click.option('--name', prompt='files name',help='file name (xlsx)')
@click.option('--n', type=int,help='n of n_gram',default= 1,prompt='ngram numbers')
def run(name,n):
    df = import_data('{}'.format(name))
    print('generate n_gram_keyword......')
    asy = get_word(n, df)
    # pprint(get_word(1,df))
    # pprint(get_mongo_data('women'))
    pool = mp.Pool()
    print('start n gram......')
    p = pool.map(get_mongo_data,asy)
    #for x in asy:
        #get_mongo_data(x)

    pprint(db.result.find_one())
    print("Done......")
    collection = db.result
    data = pd.DataFrame(list(collection.find()))
    del data['_id']
    data.to_excel('{}_gram.xlsx'.format(n), sheet_name='{}_gram'.format(n))



if __name__ == '__main__':
    run()







"""
x = db.nword2.find({"Keyword" : {'$regex' : ".*women.*"}})

click = []
imp = []
conv =[]
cost = []

for cur in x:
    pprint(cur)
    click.append(cur['Clicks'])
    imp.append(cur['Impressions'])
    conv.append(cur['Conversions'])
    cost.append(cur['Cost'])


pprint(x.count())
#pprint(x['Avg_ CPC'].sum())
#pprint(sum(cl))

#click = [ int(y['Clicks']) for y in  x ]
#cost  = [ int(c['Cost']) for c in  x]
#imp   = [ int(im['Impressions']) for im  in  x]
#conv  = [ int(con['Conversions']) for con in  x]


pprint('click : {}'.format(sum(click)))
#pprint(click)
pprint('Cost : {} '.format(round(sum(cost),2)))
pprint('imp : {} '.format(sum(imp)))
pprint(sum(conv))

#odo('mongodb://hostname/db::collection', pd.DataFrame)

"""

