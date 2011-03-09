#!/usr/bin/python
####################################################################
# FILENAME: riakidx.py
# PROJECT: Twisted Riak w/ Indexes
# DESCRIPTION: Wraps txRiak to implement transparent secondary
#       indexes in Riak.
#
#
########################################################################################
# (C)2011 DigiTar, All Rights Reserved
# Distributed under the BSD License
# 
# Redistribution and use in source and binary forms, with or without modification, 
#    are permitted provided that the following conditions are met:
#
#        * Redistributions of source code must retain the above copyright notice, 
#          this list of conditions and the following disclaimer.
#        * Redistributions in binary form must reproduce the above copyright notice, 
#          this list of conditions and the following disclaimer in the documentation 
#          and/or other materials provided with the distribution.
#        * Neither the name of DigiTar nor the names of its contributors may be
#          used to endorse or promote products derived from this software without 
#          specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY 
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES 
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT 
# SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
# DAMAGE.
#
########################################################################################

import urllib
import errors
from copy import copy
from txriak import riak
from twisted.internet import defer

# Make a copy of original RiakObject so subclassing stays stable
# when we monkey patch.
riak.RiakObjectOrig = riak.RiakObject

class RiakClientIndexed(riak.RiakClient):
    """
    Sub-class of RiakClient extended to build the index definitions.
    """
    
    def __init__(self, host='127.0.0.1', port=8098,
                prefix='riak', mapred_prefix='mapred',
                client_id=None, r_value=2, w_value=2, dw_value=0):
        """
        Construct a new RiakClientIndexed object.
        """
        
        self._indexes = {}
        riak.RiakClient.__init__(self, host, port, prefix, mapred_prefix,
                                 client_id, r_value, w_value, dw_value)
    
    def add_index(self, index):
        """
        Add a defined secondary index to the Riak client.
        
        :param index: - RiakIndex object defining the index.
        
        :returns: None
        """
        
        if not isinstance(index, RiakIndex):
            raise errors.IndexError("Not a RiakIndex instance.")
        
        if not self._indexes.has_key(index._prefix):
            self._indexes[index._prefix] = {}
        
        self._indexes[index._prefix][index._field] = index

class RiakObjectIndexed(riak.RiakObjectOrig):
    """
    RiakObjectIndex implements transparent indexing for Riak keys.
    """
    
    idx_bkt_form = "idx=%s=%s"
    idx_key_form = "%s/%s"
    
    def __init__(self, client, bucket, key=None):
        """
        Construct a new RiakObjectIndexed object.
        """
        
        self._old_data = None
        riak.RiakObjectOrig.__init__(self, client, bucket, key)
    
    @staticmethod
    def _escval(value):
        """
        Escape '=' and '/' characters in value.
        
        :param value: String value to be escaped.
        
        :returns: string
        """
        
        return urllib.quote(str(value))
    
    @staticmethod
    def _unescval(value):
        """
        Convert escaped '=' and '/' characters back to
        '=' and '/'.
        
        :param value: String value to be unescaped.
        
        :returns: string
        """
        
        return urllib.unquote(value)
    
    def set_data(self, data):
        """
        Set the data stored in this key. If the object is already
        set store the old value in *self._old_data* so we can 
        delete the old index at store time.
        
        :param data: JSON dictionary
        
        :returns: self
        """
        if self._data:
            self._old_data = copy(self._data)
        self._data = data
        
        return self
    
    @defer.inlineCallbacks
    def store(self, w=None, dw=None):
        """
        Overrides *riak.RiakObject.store()* to automatically create
        and update indexes.
        """
        
        # Store the key
        yield riak.RiakObjectOrig.store(self, w, dw)
        
        # Maintain the indexes if the data key belongs to an index
        key_prefix, key_name = self._key.split("_", 1)
        
        if self._client._indexes.has_key(key_prefix):
            
            # Maintain indexes for each indexed field
            for field in self._client._indexes[key_prefix].keys():
                idx_bucket = self.idx_bkt_form % (field, key_prefix)
                idx_bucket = self._client.bucket(idx_bucket)
            
                # Delete the old index key if there's a previous value
                if self._old_data:
                    old_value = self._escval(self._old_data[field])
                    idx_old = self.idx_key_form % (key_name, old_value)
                    idx_old = yield idx_bucket.get(idx_old)
                    yield riak.RiakObjectOrig.delete(idx_old)
            
                # Create the new index key
                new_value = self._escval(self.get_data()[field])
                idx_new = self.idx_key_form % (key_name, new_value)
                idx_new = idx_bucket.new(idx_new)
                yield riak.RiakObjectOrig.store(idx_new, w, dw)
        
        defer.returnValue(self)
    
    @defer.inlineCallbacks
    def delete(self, dw=None):
        """
        Overrides *riak.RiakObject.delete()* to automatically
        delete indexes for deleted data keys.
        """
        
        # Delete the key
        curr_data = self.get_data()
        yield riak.RiakObjectOrig.delete(self, dw)
        
        # Delete the old index key if the data key belongs to an index
        key_prefix, key_name = self._key.split("_", 1)
        if self._client._indexes.has_key(key_prefix):
            
            for field in self._client._indexes[key_prefix].keys():
                curr_value = self._escval(curr_data[field])
                idx_bucket = self.idx_bkt_form % (field, key_prefix)
                idx_bucket = self._client.bucket(idx_bucket)
            
                idx_curr = self.idx_key_form % (key_name, curr_value)
                idx_curr = yield idx_bucket.get(idx_curr)
                yield riak.RiakObjectOrig.delete(idx_curr)
        
        defer.returnValue(self)


class RiakIndex(object):
    """
    Riak secondary index object.
    
    Defines and queries a Riak secondary index.
    """
    
    def __init__(self, key_prefix, indexed_field, field_type="str"):
        """
        Define a new secondary index. Any keys stored that start with
        *key_prefix* will be detected and an index value automatically
        stored.
        
        :param key_prefix: Key prefix of keys to be included in index.
        :param indexed_field: Field name in JSON dictionary to be indexed.
        :param field_type: Data type of field.
        
        :returns: None
        """
        
        self._prefix = key_prefix
        self._field = indexed_field
        
        # Make sure field isn't a complex datatype
        field_type = str(field_type)
        if not field_type.lower() in ["int", "float", "str", "unicode"]:
            raise errors.IllegalDatatypeError(field_type)
        
        self._type = field_type
    
    def _decode_index_key(self, key_name):
        """
        Splits and decodes an index key into the data key
        name and the indexed value.
        
        :param key_name: Index key name to decode.
        
        :returns: (<data_key_name>, <indexed_value>)
        """
        
        key, value = key_name.split("/", 1)
        return (key, urllib.unquote(value))

# Install RiakObjectIndexed via monkey patch
riak.RiakObject = RiakObjectIndexed