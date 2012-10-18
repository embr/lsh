from collections import defaultdict
import numpy as np
import random
import Levenshtein
import sys
import time
import logging

logging.getLogger().setLevel(logging.INFO)

class LSHCache:

    def __init__(self, n=100, b=20, r=5, max_shingle=3):
        # assign it
        self._n = n
        self._b = b
        self._r = r
        self._max_shingle = max_shingle

        # check it
        assert self._b*self._r == self._n, 'Minhash bands/rows/length mismatch: _b*_r != _n, _b=%d, _r=%d, _n=%d' % (self._b,self._r,self._n)
        assert self._max_shingle > 0, '_max_shingle must be greater than 0.  Current _max_shingle=%d' % (self._max_shingle)

        # make it
        self._seen = set()  # the set of doc ids which have already been hashed
        self._memomask = [] # stores the random 32 bit sequences for each hash function
        self._shingles = {} # maps from words or sequences of words to integers
        self._counter = 0 # the global counter for word indicies in _shingles
        self._num_docs = 0
        self._most_recent_insert = 0
        self._init_hash_masks(self._n)
        self._cache = [defaultdict(list) for i in range(self._b)]


    def _init_hash_masks(self,num_hash):
        """
        This initializes the instance variable _memomask which is a list of the 
        random 32 bits associated with each hash function
        """
        for i in range(num_hash):
            random.seed(i)
            self._memomask.append(int(random.getrandbits(32)))

    def _xor_hash(self,mask,x):
        """
        This is a simple hash function which returns the result of a bitwise XOR
        on the input x and the 32-bit random mask
        """
        return int(x ^ mask)
        
    def _get_shingle_vec(self, doc):
        """
        Takes a sequence of tokenized words and maps each shingle to a unique id.
        These unique ids, are then added to the shingle_vec object which is just a sparse
        vector implemented as a dict with v[id]=1 when a shingle id is present
        """
        logging.debug('entering with len(doc)=%d', len(doc))
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

    def _get_sig(self,shingle_vec,num_perms):
        """
        Takes a shingle vec and computes the minhash signature of length n using
        approximate permutations.  This method is explained in Mining Massive
        Datasets by Rajaraman and Ullman (http://infolab.stanford.edu/~ullman/mmds.html)
        in section 3.3.4.
        """
        mhash = [{} for i in range(num_perms)]
        keys = sorted(shingle_vec.keys())
        for r in keys:
            #logging.debug('r=%d', r)
            h = np.array([self._xor_hash(mask,r) % len(self._shingles) for mask in self._memomask])
            for i in range(num_perms):
                if (h[i] < mhash[i]):
                    mhash[i] = h[i]
        return mhash

    def _get_lsh(self,sig,b,r):
        """
        Takes an n-dimensional minhash signature and computes b hashes for each of
        b bands of r rows in the signature.  These hashes can take on any value that
        can be stored in the 32bit integer.
        """
        lsh = []
        for i,band in enumerate(range(b)):
            lsh.append(hash(tuple(sig[i*r:i*r+r])))
        #logging.debug('hashed signature: %s\n[get_lsh]\tto bins: %s', (sig,lsh)
        return lsh
    
    def _get_lsh_from_doc(self, doc):
        """
        given an iterable of hashable items, returns a list of bucket ids
        """
        logging.debug('got tokenized doc: len(doc)=%d', len(doc))
        shingle_vec = self._get_shingle_vec(doc)
        logging.debug('got shingle_vec: len(shingle_vec)=%d', len(shingle_vec))
        sig = self._get_sig(shingle_vec,self._n) # n-dimensional min-hash signiture
        logging.debug('got minhash sig: len(sig)=%d', len(sig))
        lsh = self._get_lsh(sig,self._b,self._r) # r-dimensional list of bucket ids
        return lsh

    def _insert_lsh(self,lsh,doc_id,date_added):
        """
        Given an LSH vector of bucket indices, this method inserts the current doc
        id in the corresponding bucket for each of the _b tables
        """
        if (doc_id in self._seen):
            return
        else:
            dup_buckets = []
            self._num_docs += 1
            if (date_added > self._most_recent_insert):
                self._most_recent_insert = date_added
            self._seen.add(doc_id)
            for i,band_bucket in enumerate(lsh):
                if doc_id not in self._cache[i][band_bucket]:
                    dup_buckets.append(self._cache[i][band_bucket])
                    self._cache[i][band_bucket].append(doc_id)
            return dup_buckets

    @classmethod
    def prepare_dup_buckets(cls, buckets, id=None):
        # logging.debug('buckets: %s', buckets)
        all = list(set(reduce(list.__add__, buckets, [])))
        if id:
            all.remove(id)
        return all

    # public methods

    def get_dup_buckets(self, doc):
        """
        Returns a list of buckets (which are themselves lists) that contain the ids
        of any matching documents.  If the cache was built in chronological order
        then buckets are also in chronological order
        """
        if (id in self._seen):
            return
        if (not doc):
            print '[process_doc]\tfound empty doc, skipping'
            return
        lsh = self._get_lsh_from_doc(doc)
        dups = []
        for i,band_bucket in enumerate(lsh):
            dups.append(self._cache[i][band_bucket])
        return dups

    def get_dups(self, doc, id):
        return self.prepare_dup_buckets(self.get_dup_buckets(doc, id))

    def insert(self, doc, id, date_added=int(time.time()), passive=True):
        lsh = self._get_lsh_from_doc(doc)
        logging.debug('id: %d lsh: %s', id, lsh)
        dup_buckets = self._insert_lsh(lsh, id, date_added)
        return self.prepare_dup_buckets(dup_buckets, id=id)

    def insert_batch(self, doc_tuples):
        """Batch method for adding db docs to cache"""
        print '[add_docs]\tentering with len(docs)=%d' % (len(docs))
        for i, doc_tuple in enumerate(doc_tupless):
            if (i % 100 == 0):
                print '\r[add_docs]\tprocessed %d / %d docs:' % (i,len(docs)),
            dup_buckets[i] = self.insert(*doc_tuple)
        return dup_buckets

    def num_docs(self):
        return self._num_docs

    def most_recent_insert(self):
        return self._most_recent_insert

    def num_shingles(self):
        return self._counter

