from collections import defaultdict
import numpy as np
import random
import Levenshtein
import sys

"""
Takes a row from a mongodb query and creates a list of tokenized of words from the url,
title, rss-content and extended content along with the ObjectId object corresponding to
that doc, and the unix time stamp of when that document was added to the db
"""
def prepare_doc(doc,fields=('URL','Title','Content','ExtendedContent')):
    try:
        prepared = []
        doc_id = doc['_id']
        date_added = doc['DateAdded']
        for field in fields:
            if doc.has_key(field) and doc[field]:
                field_val = doc[field]
                toks = field_val.split()
                for tok in toks:
                    prepared.append(tok.strip())
            else:
                print '[WARNING]\t[prepare_doc]\tfound doc with no field: %s' % (field)
        return (doc_id,date_added,prepared)
    except:
        print '[WARNING]\t[prepare_doc]\terror: %s' % (sys.exc_info()[0])
        return (None,None,None)


class LSHCache:

    _cache = []
    _seen = set() # a set of pymongo.ObjectId's which stores db object which have already been hashed
    _memomask = [] # stores the random 32 bit sequences for each hash function
    _shingles = {} # maps from words or sequences of words to integers
    _counter = 0 # the global counter for word indicies in _shingles
    _n = 0 # the dimensionality of the minhash signature: the number of permutations
    _b = 0 # the number of bands for LSH: the number of tables in the cache
    _r = 0 # the number of rows per band.  Note: _b*_r = _n
    _max_shingle = 0
    _min_jaccard = 0
    _max_edit_rate = 0
    _min_doc_len = 0
    _num_docs = 0
    _most_recent_insert = 0

    def __init__(self,args):
        print '[__init__]\tentering'
        if (args.cache):
            init_from_file(args.cache)
        else:
            print '[__init__]\tbuilding cache from scratch'
            # assign it
            self._n = args.n
            self._b = args.b
            self._r = args.r
            self.init_hash_masks(self._n)
            self._max_shingle = args.max_shingle
            self._min_jaccard = args.min_jaccard
            self._max_edit_rate = args.max_edit_rate
            self._min_doc_len = args.min_doc_len

            # check it
            assert self._b*self._r == self._n, 'Minhash bands/rows/length mismatch: _b*_r != _n, _b=%d, _r=%d, _n=%d' % (self._b,self._r,self._n)
            assert self._max_shingle > 0, '_max_shingle must be greater than 0.  Current _max_shingle=%d' % (self._max_shingle)
            assert (self._min_jaccard <= 1 and self._min_jaccard > 0), '_min_jaccard must be in the interval (0,1].  Current, _min_jaccard=%f' % (self._min_jaccard)
            assert (self._max_edit_rate <= 1 and self._max_edit_rate > 0), '_max_edit_rate must be in the interval (0,1].  Current, _max_edit_rate=%f' % (self._max_edit_rate)
            assert (self._min_doc_len >= 0), '_min_doc_len must be >=0.  Current _min_doc_len=%d' % (self._min_doc_len)
            
            self._cache = [defaultdict(list) for i in range(self._b)]


    """
    This initializes the instance variable _memomask which is a list of the 
    random 32 bits associated with each hash function
    """
    def init_hash_masks(self,num_hash):
        for i in range(num_hash):
            random.seed(i)
            self._memomask.append(int(random.getrandbits(32)))

    """
    This is a simple hash function which returns the result of a bitwise XOR
    on the input x and the 32-bit random mask
    """
    def xor_hash(self,mask,x):
        return int(x ^ mask)
        
    """
    Takes a sequence of tokenized words and maps each shingle to a unique id.
    These unique ids, are then added to the shingle_vec object which is just a sparese
    vector implemented as a dict with v[id]=1 when a shingle id is present
    """
    def get_shingle_vec(self, doc):
        #print '[get_shingle_vec]\tentering with len(doc)=%d' % (len(doc))
        v = {}
        for n in range(self._max_shingle):
            doc.insert(0,'<start>')
            for j in range(len(doc) - n):
                s = doc[j:j+n]
                if not self._shingles.has_key(tuple(s)):
                    self._shingles[tuple(s)] = self._counter
                    self._counter += 1
                v[self._shingles[tuple(s)]] = 1
        return v

    """
    Takes a shingle vec and computes the minhash signature of length n using
    an approximate permutations.  This method is explained in Mining Massive
    Datasets by Rajaraman and Ullman (http://infolab.stanford.edu/~ullman/mmds.html)
    in section 3.3.4.
    """
    def get_sig(self,shingle_vec,num_perms):
        mhash = [{} for i in range(num_perms)]
        keys = sorted(shingle_vec.keys())
        for r in keys:
            #print '[get_sigs]\tr=%d' % (r)
            h = np.array([self.xor_hash(mask,r) % len(self._shingles) for mask in self._memomask])
            for i in range(num_perms):
                if (h[i] < mhash[i]):
                    mhash[i] = h[i]
        return mhash

    """
    Takes an n-dimensional minhash signature and computes _b hash for each of
    _b bands of _r rows in the signature.  These hashes can take on any value that
    can be stored in the 32bit integer.
    """
    def get_lsh(self,sig,b,r):
        lsh = []
        for i,band in enumerate(range(b)):
            lsh.append(hash(tuple(sig[i*r:i*r+r])))
        #print '[get_lsh]\thashed signature: %s\n[get_lsh]\tto bins: %s' % (sig,lsh)
        return lsh
    
    """Returns a list of buckets (which are themselves lists) which contain the ids
       of any matching documents.  If the cache was built in chronological order,
       the first element in the bucket list can be treated as the original document"""
    def get_dup_buckets(self,lsh,all=False,min_jaccard=0.0,max_edit_rate=1):
        dups = []
        for i,band_bucket in enumerate(lsh):
            dups.append(self._cache[i][band_bucket])
        return dups

    """Given an LSH vector of bucket indices, this method inserts the current doc
       id in the corresponding bucket for each of the _b tables"""
    def insert_doc(self,lsh,doc_id,date_added):
        if (doc_id in self._seen):
            return
        else:
            self._num_docs += 1
            if (date_added > self._most_recent_insert):
                self._most_recent_insert = date_added
            self._seen.add(doc_id)
            for i,band_bucket in enumerate(lsh):
                if doc_id not in self._cache[i][band_bucket]:
                    self._cache[i][band_bucket].append(doc_id)

    """Given an SQL row, go through the whole pipeline and either insert the doc
       in the appropriate buckets, or if passive=True, just return the list of
       buckets which match the probe document"""
    def process_doc(self,doc,passive=True):
        #print '[process_doc]\tentering'
        [doc_id,date_added,doc] = prepare_doc(doc)
        if (doc_id in self._seen):
            return
        if (not doc):
            print '[process_doc]\tfound empty doc, skipping'
            return
        #print '[process_doc]\got tokenized doc: len(doc)=%d' % (len(doc))
        shingle_vec = self.get_shingle_vec(doc)
        #print '[process_doc]\got shingle_vec: len(shingle_vec)=%d' % (len(shingle_vec))
        sig = self.get_sig(shingle_vec,self._n) # n-dimensional min-hash signiture
        #print '[process_doc]\got minhash sig: len(sig)=%d' % (len(sig))
        lsh = self.get_lsh(sig,self._b,self._r) # r-dimensional list of bucket ids
        #print '[process_doc]\got lsh bucket ids: len(lsh)=%d' % (len(lsh))
        # actually insert the doc in the cache according to LSH
        dup_buckets = self.get_dup_buckets(lsh)
        #print '[process_doc]\found the following duplicate buckets:\n\t%s' % (dup_buckets)
        if (not passive):
            #print '[process_doc]\tinserting doc'
            self.insert_doc(lsh,doc_id,date_added)
        return (doc_id,dup_buckets)

    """Batch method for adding db docs to cache"""
    def process_docs(self,docs,passive=True):
        print '[add_docs]\tentering with len(docs)=%d' % (len(docs))
        for i,doc in enumerate(docs):
            if (i % 100 == 0):
                print '\r[add_docs]\tprocessed %d / %d docs:' % (i,len(docs)),
            [doc_id, dup_buckets] = self.process_doc(doc,passive)

    """Writes a duplicate data structure to file as a tsv of the format
       id1    id2    score
       Expects dups to be a list of tuples(tuple(id1,id2),score) where score
       is either a jaccard similarity or edit distance rate"""
    def write_dups(self,dups,out_fname):
        f = open(out_fname, 'w')
        for ids, jaccard in dups.items():
            [id1,id2] = ids
            f.write(str(id1)+'\t'+str(id2)+'\t'+str(jaccard)+'\n')
        ff.close()

    """Computes the mean similarity for a  duplicate data structure.
       Expects dups to be a list of tuples(tuple(id1,id2),score) where score
       is either a jaccard similarity or edit distance rate"""
    def analyze_dups(self,dups,docs):
        print 'mean score: %f' % (np.mean(dups.values()))
        print 'n: %d' % (len(dups))                

    def num_docs(self):
        return self._num_docs

    def most_recent_insert(self):
        return self._most_recent_insert

    def num_shingles(self):
        return self._counter

