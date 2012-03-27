#!/usr/bin/python
import lsh
import Levenshtein

import argparse
import sys
import time
import pickle

import MySQLdb as sql
import pymongo

import cProfile
import pstats


"""
This provides a CLI for the lsh module, which implements a Locality-Sensitive Hashing system to efficiently detect
duplicate reports.  It is currently designed to operate like a periodic background job.  To create a new cache and
index all of the documents in the Report collection added after <unix_time>, call:

    lsh_app.py -save <save_name> -start <unix_time>

then, to update the cache with any reports added to the db since the last document added to the lsh cache, call:

    lsh_app.py -cache <save_name>

"""

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-cache', type=unicode, default=None, help='The location of the cache file to use. If provided, the program will run in worker mode, waiting for duplicate check requests against the specified cache file.')
    parser.add_argument('-save', type=unicode, default=u'anon_'+unicode(int(time.time())), help='Save name for cache file. If loading from saved cache, updated cache will be saved to same location.  Otherwise, default is anon_<unix_time>.cache')
    parser.add_argument('-start', type=int, default=None, help='If cache file not supplied, this arg specifies the earliest time from which to build a new cache.  value is given as unix time')
    parser.add_argument('-passive', action='store_true', default=False,  help='checks docs against cache without adding those docs to cache')
    parser.add_argument('-tag', type=str, default=unicode(int(time.time())), help='uses this name')
    parser.add_argument('-n', type=int, default=100, help='Number of hashes per document. Note, b*n==r.')
    parser.add_argument('-b', type=int, default=20, help='Number of bins in cache. Note, b*n==r.')
    parser.add_argument('-r', type=int, default=5, help='Number of rows per bin. Note, b*n==r.')
    parser.add_argument('-max_shingle', type=int, default=3, help='Maximum shingle size.)')
    parser.add_argument('-min_jaccard', type=float, default=0.8, help='minimum jaccard similarity to judge as duplicate')
    parser.add_argument('-max_edit_rate', type=float, default=.1, help='Maximum edit distance rate (edit_dist(doc1,doc2)/min(len(doc1),len(doc2))) to judge as a duplicate')
    parser.add_argument('-min_doc_len', type=int, default=100, help='Minimum doc length for fuzzy matching.')
    parser.add_argument('-db_host', type=unicode, default="localhost",help='mongodb hostname/ip')
    parser.add_argument('-db_name', type=unicode, default=None,help='mongodb db name')
    parser.add_argument('-coll_name', type=unicode, default="Report",help='mongodb collection name with db specified by -db_name arg')

    args = parser.parse_args()
    print args
    return args

"""
Returns a mongo collection object for the db_host/db_name/coll_name collections
"""
def get_coll(db_host,db_name,coll_name):
    conn = pymongo.Connection(db_host)
    print '[get_coll]\tgot connection db_host: %s' % (db_host)
    db = conn[db_name]
    coll = db[coll_name]
    print '[get_coll]\tgot collection: epidemiciq.Report, count({})=%d' % (coll.count())
    return coll

"""
Computes the jaccard SIMILARITY between two tokenized lists
"""
def jaccard_sim(doc1,doc2):
    s1 = set(doc1)
    s2 = set(doc2)
    i = s1.intersection(s2)
    u = s1.union(s2)
    return float(len(i))/float(len(u))

"""
Given a mongodb document, doc, and a set of candidate object_ids (mongo ids that is), 
this method returns the earliest doc_id which satisfies ALL OF the jaccard simmilarity
threshold, the Levenshtein edit distance rate threshold, and the minimum document length
threshold.  It returns None, if no such document can be found
"""
def get_dup(coll,doc,cand_buckets,jac_thres,edit_rate_thres,min_doc_len):
    dup_id = None
    [doc_id,date_added,doc_prepared] = lsh.prepare_doc(doc)
    if (len(doc_prepared) < min_doc_len):
        return dup_id
    ids = []
    for bucket in cand_buckets:
        for cand_id in bucket:
            ids.append(cand_id)
    ids_unique = set(ids)
    if (doc_id in ids_unique):
        ids_unique.remove(doc_id)
    ids_unique = list(ids_unique)
    query = {"_id" : {"$in" : ids_unique}}
    sort_obj = [('DateAdded',pymongo.DESCENDING)]
    cand_docs = coll.find(query,sort=sort_obj)
    for cand_doc in cand_docs:
        [cand_id,cand_date_added,cand_doc_prepared] = lsh.prepare_doc(cand_doc)
        jac = jaccard_sim(doc_prepared,cand_doc_prepared)
        if (jac > jac_thres):
            edit = Levenshtein.distance(' '.join(doc_prepared),' '.join(cand_doc_prepared))
            edit_rate = float(edit) / min(len(doc),len(cand_doc_prepared))
            if (edit_rate < edit_rate_thres):
                dup_id = cand_id
    return dup_id

"""
This method is the main entry point and first parses the arguments, then creates an LSHCache
object.  After loading or creating a new cache, it then queries the database for any reports
added since the cache was last updated, and inserts all returned documents.
"""
def main():
    args=parse_args()
    cache = ()
    if args.cache: # load from file
        fin = open(args.cache, 'r')
        cache = pickle.load(fin)
    else: # create a new object
        cache = lsh.LSHCache(args)

    # create db object
    coll = get_coll(args.db_host,args.db_name,args.coll_name)

    # determines minimum time for query
    start_time = args.start
    if not start_time:
        # default for new cache is 0, so we get everything
        start_time = cache.most_recent_insert()

    query = {"ExtendedContent" : {"$nin" : ["null",""], "$exists" : True, "$type" : 2}, "DateAdded" : {"$gt" : start_time} }
    sort_obj = [('DateAdded',pymongo.ASCENDING)]
    print '[main]\tissuing query: %s' % (query)
    r = coll.find(query,sort=sort_obj)
    total = r.count()
    for i,doc in enumerate(r):
        if (i % 100 == 0):
            print '[main]\tprocessed %d / %d rows' % (i,total)
        [doc_id,cand_buckets] = cache.process_doc(doc,args.passive)
        dup_id = get_dup(coll,doc,cand_buckets,args.min_jaccard,args.max_edit_rate,args.min_doc_len)
        if (dup_id and not args.passive):
            # write to db
            print 'writing to db for id pair: %s -> %s' % (doc_id,dup_id)
            selector_object = {'_id' : doc_id}
            update_object = {'$push' : {'DuplicateOf' : dup_id} }
            #coll.update(selector_object,update_object)
        #print dup_buckets

    print '[main]\tcache.num_docs()=%d' % (cache.num_docs())
    print '[main]\tcache.num_shingles()=%d' % (cache.num_shingles())
    fout_name = args.cache
    if not fout_name:
        fout_name = args.save+'.cache.pickle'
    fout = open(fout_name,'w')
    pickle.dump(cache,fout)
    fout.close()

############## SCRIPT BEGINS HERE ###################

main()

#profiling stuff
#cProfile.run('main()','prof.txt')
#p = pstats.Stats('prof.txt')
#p.strip_dirs().sort_stats(-1).print_stats()
#p.sort_stats('cumulative').print_stats(10)
