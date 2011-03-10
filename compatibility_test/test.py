#!/usr/bin/python
# -*- coding: utf-8 -*-
####################################################################
# FILENAME: test.py
# PROJECT: Twisted Riak w/ Indexes
# DESCRIPTION: Tests the compatibility of other implementations of
#              this indexing strategy.
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

import json, urllib
from argparse import ArgumentParser
from twisted.internet import reactor, defer
from txriakidx import riakidx

# Test JSON
sample_json = '{"boolean": true, "integer": 1, "float": 4.0999999999999996, "string": "test", "unicode": "\u65e5\u672c\u4eba!"}'

# Build arguments
parser = ArgumentParser()
parser.add_argument("--server", dest="server", required=True)
parser.add_argument("--port", dest="port", type=int, required=True)
parser.add_argument("--bucket", dest="bucket", default="test_bucket")
parser.add_argument("--prefix", dest="prefix", default="prefix")
parser.add_argument("--keyname", dest="keyname", default="test_key")
parser.add_argument("--load", dest="load", action="store_true", default=False)
parser.add_argument("--validate", dest="validate", action="store_true", default=False)

@defer.inlineCallbacks
def load_reference_samples(result, host, port, bucket, prefix, keyname):
    "Load the sample record and create reference indexes."
    
    client = riakidx.RiakClient(host, port)
    bkt = client.bucket(bucket)
    
    idx_string = riakidx.RiakIndex(bucket=bucket,
                                   key_prefix=prefix,
                                   indexed_field="string",
                                   field_type="str")
    
    idx_integer = riakidx.RiakIndex(bucket=bucket,
                                    key_prefix=prefix,
                                    indexed_field="integer",
                                    field_type="int")
    
    idx_float = riakidx.RiakIndex(bucket=bucket,
                                  key_prefix=prefix,
                                  indexed_field="float",
                                  field_type="float")
    
    idx_bool = riakidx.RiakIndex(bucket=bucket,
                                 key_prefix=prefix,
                                 indexed_field="boolean",
                                 field_type="bool")
    
    idx_unicode = riakidx.RiakIndex(bucket=bucket,
                                    key_prefix=prefix,
                                    indexed_field="unicode",
                                    field_type="unicode")
    
    client.add_index(idx_string)
    client.add_index(idx_integer)
    client.add_index(idx_float)
    client.add_index(idx_bool)
    client.add_index(idx_unicode)
    
    yield bkt.new("%s_%s" % (prefix, keyname), json.loads(sample_json)).store()
    

@defer.inlineCallbacks
def reference_validate(result, host, port, bucket, prefix, keyname):
    "Validate indexes for keyname against reference."
    
    client = riakidx.RiakClient(host, port)
    
    print "Validating integer index...",
    integer_bkt = client.bucket("idx=%s=%s=integer" % (bucket, prefix))
    integer_idx = yield integer_bkt.get("%s/1" % keyname)
    if integer_idx.exists():
        print "good."
    else:
        print "failed. Should be %s%%2F1" % keyname
        
    print "Validating string index...",
    string_bkt = client.bucket("idx=%s=%s=string" % (bucket, prefix))
    string_idx =  yield string_bkt.get("%s/test" % keyname)
    if string_idx.exists():
        print "good."
    else:
        print "failed. Should be %s%%2Ftest" % keyname
    
    
    print "Validating bool index...",
    bool_bkt = client.bucket("idx=%s=%s=boolean" % (bucket, prefix))
    bool_idx = yield bool_bkt.get("%s/1" % keyname)
    if bool_idx.exists():
        print "good."
    else:
        print "failed. Should be %s%%2F1" % keyname
    
    print "Validating float index...",
    float_bkt = client.bucket("idx=%s=%s=float" % (bucket, prefix))
    float_idx = yield float_bkt.get("%s/4.1" % keyname)
    if float_idx.exists():
        print "good."
    else:
        print "failed. Should be %s%%2F4.1" % keyname
    
    print "Validating unicode index...",
    unicode_bkt = client.bucket("idx=%s=%s=unicode" % (bucket, prefix))
    unicode_idx =  yield unicode_bkt.get(keyname + "/" + urllib.quote(u"日本人!".encode("utf-8")))
    if unicode_idx.exists():
        print "good."
    else:
        print "failed. Should be " + keyname + "%2F%25E6%2597%25A5%25E6%259C%25AC%25E4%25BA%25BA%2521"

def all_done(result):
    reactor.stop()


def eb_failed(failure):
    failure.printTraceback()
    reactor.stop()

if __name__ == "__main__":
    args = parser.parse_args()
    d = defer.Deferred()

    if args.load:
        d.addCallback(load_reference_samples,
                      args.server,
                      args.port,
                      args.bucket,
                      args.prefix,
                      args.keyname)

    if args.validate:
         d.addCallback(reference_validate,
                       args.server,
                       args.port,
                       args.bucket,
                       args.prefix,
                       args.keyname)

    d.addCallback(all_done)
    d.addErrback(eb_failed)

    reactor.callLater(0, d.callback, True)
    reactor.run()



