#!/usr/bin/python
####################################################################
# FILENAME: test_riakidx.py
# PROJECT: Twisted Riak w/ Indexes
# DESCRIPTION: Tests for main txRiakIdx classes
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
#          used to endorse or promote products derived from this software without g
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

import urllib, copy, time
from twisted.trial import unittest
from twisted.internet import defer
from txriak import riak
from txriakidx import riakidx
from txriakidx import errors


class RiakIdxPseudoTestCase(unittest.TestCase):
    """
    Parent test-case for abstracting setup and teardown.
    """
    
    def setUp(self):
        self.client = riakidx.RiakClient()
        self.bucket = self.client.bucket("test_bucket")
        self.sample_record = {"string": "test!",
                              "integer" : 50,
                              "float" : 3.14,
                              "unicode" : u"test some more!"}
        self.riak_keys = ["prefix_testkey",
                          "prefix_key1",
                          "prefix_key2",
                          "prefix_key3",
                          "prefix_key4"]
    
    @defer.inlineCallbacks
    def tearDown(self):
        for key in self.riak_keys:
            obj = yield self.bucket.get(key)
            if obj.exists():
                yield obj.delete()

class RiakIndexTestCase(RiakIdxPseudoTestCase):
    """
    Test cases for RiakIndex.
    """
    
    def test_create_index_instance_ok(self):
        "Validate RiakIndex instance with allowed field datatype."
        idx = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                key_prefix="prefix",
                                indexed_field="field",
                                field_type="int")
        self.assertEqual(idx._prefix, "prefix")
        self.assertEqual(idx._field, "field")
        self.assertEqual(idx._type, "int")
        self.assertEqual(idx._client, None)
        
    
    def test_create_index_instance_bad_datatype(self):
        "Validate RiakIndex raises error on invalid index field datatype."
        self.assertRaises(errors.IllegalDatatypeError,
                          riakidx.RiakIndex,
                          bucket=self.bucket.get_name(),
                          key_prefix="prefix",
                          indexed_field="field",
                          field_type="dict")
    
    def test_decode_index_key(self):
        "Validates index key decoding."
        idx = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                key_prefix="prefix",
                                indexed_field="field",
                                field_type="int")
        key, val = idx._decode_index_key("testkey/test%21")
        self.assertEqual(key, "testkey")
        self.assertEqual(val, "test!")
    
    def test_query_no_client(self):
        "Validate running query before adding to a client fails."
        idx = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                key_prefix="prefix",
                                indexed_field="field",
                                field_type="int")
        self.assertFailure(idx.query("", ""), errors.IndexError)
    
    @defer.inlineCallbacks
    def test_query_string_match(self):
        "Test querying index for match against string field"
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        # Generate & load the keys
        test_keys = {"key1" : "test!",
                     "key2" : "test!",
                     "key3" : "hello",
                     "key4" : "hello-yo"}
        
        for key in test_keys.keys():
            contents = {"string" : test_keys[key]}
            obj = yield self.bucket.new("prefix_" + key, contents).store()
        
        # Find all keys in the index matching "test" 
        # for the 'string' field.
        result = yield idx1.query("eq", "test!")
        actual_result = sorted(result)
        
        expected_result = sorted([[u"test_bucket", u"prefix_key1", u"test!"],
                                  [u"test_bucket", u"prefix_key2", u"test!"]])
        self.assertEqual(expected_result, actual_result)
    
    @defer.inlineCallbacks
    def test_query_integer_match(self):
        "Test querying index for match against integer field"
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="integer",
                                 field_type="int")
        self.client.add_index(idx1)
        
        # Generate & load the keys
        test_keys = {"key1" : 3,
                     "key2" : 3,
                     "key3" : 4,
                     "key4" : 4}
        
        for key in test_keys.keys():
            contents = {"integer" : test_keys[key]}
            obj = yield self.bucket.new("prefix_" + key, contents).store()
        
            # Find all keys in the index with value < 4
            # for the 'integer' field.
        result = yield idx1.query("less_than", 4)
        actual_result = sorted(result)
        
        expected_result = sorted([[u"test_bucket", u"prefix_key1", 3],
                                  [u"test_bucket", u"prefix_key2", 3]])
        self.assertEqual(expected_result, actual_result)
    
    @defer.inlineCallbacks
    def test_query_boolean_match(self):
        "Test querying index for match against boolean field"
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="boolean",
                                 field_type="bool")
        self.client.add_index(idx1)
        
        # Generate & load the keys
        test_keys = {"key1" : True,
                     "key2" : True,
                     "key3" : False,
                     "key4" : False}
        
        for key in test_keys.keys():
            contents = {"boolean" : test_keys[key]}
            obj = yield self.bucket.new("prefix_" + key, contents).store()
        
        # Find all keys in the index with value
        # True.
        result = yield idx1.query("eq", True)
        actual_result = sorted(result)
        
        expected_result = sorted([[u"test_bucket", u"prefix_key1", True],
                                  [u"test_bucket", u"prefix_key2", True]])
        self.assertEqual(expected_result, actual_result)
    
    @defer.inlineCallbacks
    def test_query_float_match(self):
        "Test querying index for match against float field"
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="float",
                                 field_type="float")
        self.client.add_index(idx1)
        
        # Generate & load the keys
        test_keys = {"key1" : 3.14,
                     "key2" : 3.14,
                     "key3" : 4.14,
                     "key4" : 4.0}
        
        for key in test_keys.keys():
            contents = {"float" : test_keys[key]}
            obj = yield self.bucket.new("prefix_" + key, contents).store()
        
        # Find all keys in the index with value < 4
        # for the 'float' field.
        result = yield idx1.query("less_than", 4)
        actual_result = sorted(result)
        
        expected_result = sorted([[u"test_bucket", u"prefix_key1", 3.14],
                                  [u"test_bucket", u"prefix_key2", 3.14]])
        self.assertEqual(expected_result, actual_result)
    
    @defer.inlineCallbacks
    def test_query_invalid_compare_op(self):
        "Test querying index using an illegal comparison/predicate operation."
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        # Generate & load the keys
        test_keys = {"key1" : "test!",
                     "key2" : "test!",
                     "key3" : "hello",
                     "key4" : "hello-yo"}
        
        for key in test_keys.keys():
            contents = {"string" : test_keys[key]}
            obj = yield self.bucket.new("prefix_" + key, contents).store()
        
        # Make sure illegal compare ops raise IndexError
        yield self.assertFailure(idx1.query("my_bizarro_opprint", "test!"),
                                 errors.IndexError)

class RiakClientTestCase(RiakIdxPseudoTestCase):
    """
    Tests cases for RiakClient
    """
    
    def test_create_client(self):
        "Validate basic client initialization."
        self.assertEqual(self.client._indexes, {})
    
    def test_add_index_ok(self):
        "Add an index to the client successfully."
        bucket = self.bucket.get_name()
        idx = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                key_prefix="testpref",
                                indexed_field="field_1",
                                field_type="int")
        self.client.add_index(idx)
        self.assertTrue(self.client._indexes.has_key(bucket+"=testpref"))
        self.assertTrue(self.client._indexes[bucket+"=testpref"].has_key("field_1"))
        self.assertEqual(idx._client, self.client)
        self.assertEqual(idx, self.client._indexes[bucket+"=testpref"]["field_1"])
    
    def test_add_index_failed(self):
        "Add an invalid index to the client...fails."
        idx = "this ain't an index"
        self.assertRaises(errors.IndexError, self.client.add_index, idx)
    

class RiakObjectTestCase(RiakIdxPseudoTestCase):
    """
    Test cases for RiakObject
    """
    
    idx_bkt_form = "idx=%(bucket)s=%(key_prefix)s=%(field)s"
    idx_key_form = "%(key)s/%(field_value)s"
    
    @defer.inlineCallbacks
    def test_create_object_ok(self):
        "Create a new object and associated index."
        
        # Setup index definition
        idx = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                key_prefix="prefix",
                                indexed_field="string",
                                field_type="str")
        self.client.add_index(idx)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key & index stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        index_bucket = self.idx_bkt_form % {"bucket": self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        key, val = idx._decode_index_key(obj_idx_test.get_key())
        self.assertEqual("testkey", key)
        self.assertEqual(self.sample_record["string"], val)
    
    @defer.inlineCallbacks
    def test_create_object_ok_multiple_field_indexes(self):
        "Create a new object with indexes on multiple fields"
        
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        idx2 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="integer",
                                 field_type="int")
        self.client.add_index(idx1)
        self.client.add_index(idx2)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        # Validate index 1 stored properly
        index_bucket = self.idx_bkt_form % {"bucket": self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        key, val = idx1._decode_index_key(obj_idx_test.get_key())
        self.assertEqual("testkey", key)
        self.assertEqual(self.sample_record["string"], val)
        
        # Validate index 2 stored properly
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "integer",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(str(self.sample_record["integer"]))}
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        key, val = idx2._decode_index_key(obj_idx_test.get_key())
        self.assertEqual("testkey", key)
        self.assertEqual(str(self.sample_record["integer"]), val)
    
    def test_escape_field_value(self):
        "Test escaping index field values."
        field_val = "my_utterly/obfuscated!key="
        field_encoded_val = "my_utterly%2Fobfuscated%21key%3D"
        self.assertEqual(field_encoded_val,
                         riakidx.RiakObject._escval(field_val))
    
    def test_unescape_field_value(self):
        "Test unescaping index field values."
        field_val = "my_utterly/obfuscated!key="
        field_encoded_val = "my_utterly/obfuscated%21key%3D"
        self.assertEqual(field_val,
                         riakidx.RiakObject._unescval(field_encoded_val))
    
    @defer.inlineCallbacks
    def test_store_noindexes(self):
        "Test storing a key with no indexes makes no indexes."
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="noprefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        # Validate no index was created
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "noprefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertFalse(obj_idx_test.exists())

        
    @defer.inlineCallbacks
    def test_store_index_noprevindex_newindexclash(self):
        "Test storing a new key and index where the new index already exists."
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        # Pre-create the index key
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        pre_idx_bucket = self.client.bucket(index_bucket)
        pre_idx = pre_idx_bucket.new(index_key)
        yield riak.RiakObjectOrig.store(pre_idx)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        # Validate index was created
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
    
    @defer.inlineCallbacks
    def test_store_index_previndex_ok(self):
        "Test storing a key with a new value, where a previous index existed."
        # Create key and index the first time
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key_old = self.idx_key_form % {"key" : "testkey",
                                             "field_value" : urllib.quote(self.sample_record["string"])}
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key_old)
        self.assertTrue(obj_idx_test.exists())
        
        # Update key and index
        updated_record = copy.deepcopy(self.sample_record)
        updated_record["string"] = "testing!"
        obj = yield self.bucket.get("prefix_testkey")
        obj.set_data(updated_record)
        yield obj.store()
        
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(updated_record, obj_test.get_data())
        
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(updated_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key_old)
        self.assertFalse(obj_idx_test.exists())
    
    @defer.inlineCallbacks
    def test_store_index_previndex_previndexmissing(self):
        "Test updating a key and index, but the old index key is missing."
        # Create key and index the first time
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key_old = self.idx_key_form % {"key" : "testkey",
                                             "field_value" : urllib.quote(self.sample_record["string"])}
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key_old)
        self.assertTrue(obj_idx_test.exists())
        
        # Delete the old index
        obj_idx_test = yield idx_bucket.get(index_key_old)
        yield riak.RiakObjectOrig.delete(obj_idx_test)
        
        # Update key and index
        updated_record = copy.deepcopy(self.sample_record)
        updated_record["string"] = "testing!"
        obj = yield self.bucket.get("prefix_testkey")
        obj.set_data(updated_record)
        yield obj.store()
        
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(updated_record, obj_test.get_data())
        
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(updated_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key_old)
        self.assertFalse(obj_idx_test.exists())
    
    @defer.inlineCallbacks
    def test_delete_noindexes(self):
        "Test deleting a key that has no indexes."
         # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="noprefix",
                                 indexed_field="string",
                                 field_type="str")
        self.client.add_index(idx1)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        # Validate no index was created
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "noprefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertFalse(obj_idx_test.exists())
        
        # Delete the key
        yield obj_test.delete()
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertFalse(obj_test.exists())
    
    @defer.inlineCallbacks
    def test_delete_previndex_ok(self):
        "Test deleting a key that has indexes."
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        idx2 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="integer",
                                 field_type="int")
        self.client.add_index(idx1)
        self.client.add_index(idx2)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        # Validate indexes were created
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "integer",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(str(self.sample_record["integer"]))}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertTrue(obj_idx_test.exists())
        
        # Delete the key
        yield obj_test.delete()
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertFalse(obj_test.exists())
        
        # Validate indexes are gone
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertFalse(obj_idx_test.exists())
        
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "integer",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(str(self.sample_record["integer"]))}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertFalse(obj_idx_test.exists())
    
    @defer.inlineCallbacks
    def test_delete_previndex_previndex_missing(self):
        "Test deleting a key that should have indexes but they're missing."
        # Setup index definition
        idx1 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="string",
                                 field_type="str")
        idx2 = riakidx.RiakIndex(bucket=self.bucket.get_name(),
                                 key_prefix="prefix",
                                 indexed_field="integer",
                                 field_type="int")
        self.client.add_index(idx1)
        self.client.add_index(idx2)
        
        # Store object
        obj = self.bucket.new("prefix_testkey", self.sample_record)
        yield obj.store()
        
        # Validate key stored properly
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertEqual(self.sample_record, obj_test.get_data())
        
        # Validate indexes were created
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key_1 = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key_1)
        self.assertTrue(obj_idx_test.exists())
        
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "integer",
                                            "key_prefix" : "prefix"}
        index_key_2 = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(str(self.sample_record["integer"]))}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key_2)
        self.assertTrue(obj_idx_test.exists())
        
        # Delete the indexes
        obj_idx_test = yield idx_bucket.get(index_key_1)
        yield riak.RiakObjectOrig.delete(obj_idx_test)
        obj_idx_test = yield idx_bucket.get(index_key_2)
        yield riak.RiakObjectOrig.delete(obj_idx_test)
        
        # Delete the key
        yield obj_test.delete()
        obj_test = yield self.bucket.get("prefix_testkey")
        self.assertFalse(obj_test.exists())
        
        # Validate indexes are gone
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "string",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(self.sample_record["string"])}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertFalse(obj_idx_test.exists())
        
        index_bucket = self.idx_bkt_form % {"bucket" : self.bucket.get_name(),
                                            "field" : "integer",
                                            "key_prefix" : "prefix"}
        index_key = self.idx_key_form % {"key" : "testkey",
                                         "field_value" : urllib.quote(str(self.sample_record["integer"]))}
        idx_bucket = self.client.bucket(index_bucket)
        obj_idx_test = yield idx_bucket.get(index_key)
        self.assertFalse(obj_idx_test.exists())
