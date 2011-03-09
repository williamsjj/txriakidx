#!/usr/bin/python
####################################################################
# FILENAME: orders_with_indexes.py
# PROJECT: Twisted Riak w/ Indexes
# DESCRIPTION: Example showing indexes on diner orders.
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

from twisted.internet import reactor
from twisted.internet import defer

from txriakidx import riakidx

BUCKET = "diner_bucket"
ORDER_KEY_PREFIX = "order"

order1 = {"order_number" : 123456789,
          "diner_name" : "Bobbie Jo Rickelbacker"}

order2 = {"order_number" : 234234293,
          "diner_name" : "Joe Blow Rickelbacker"}


@defer.inlineCallbacks
def create_orders(result):
    
    # Create client
    client = riakidx.RiakClient()
    
    # Define indexes & register them with the client
    idx1 = riakidx.RiakIndex(bucket=BUCKET,
                             key_prefix=ORDER_KEY_PREFIX,
                             indexed_field="order_number",
                             field_type="int")
    
    idx2 = riakidx.RiakIndex(bucket=BUCKET,
                             key_prefix=ORDER_KEY_PREFIX,
                             indexed_field="diner_name",
                             field_type="str")
    
    client.add_index(idx1) # NB: Without this step, your indexes
    client.add_index(idx2) #     won't be added, updated or deleted.
    
    # Create our orders (indexes will happen automagically!)
    # (Your key prefix and key name must be separated by a _
    #  ...that's how txRiakIdx identifies the prefix in the 
    # key name.)
    bucket = client.bucket(BUCKET)
    
    obj1 =  bucket.new(ORDER_KEY_PREFIX + "_" + "order1", order1)
    yield obj1.store()
    
    obj2 = bucket.new(ORDER_KEY_PREFIX + "_" + "order2", order2)
    yield obj2.store()
    
    return

@defer.inlineCallbacks
def show_orders(result):
    
    # Create client
    client = riakidx.RiakClient()
    
    # Define indexes & register them with the client
    idx1 = riakidx.RiakIndex(bucket=BUCKET,
                             key_prefix=ORDER_KEY_PREFIX,
                             indexed_field="order_number",
                             field_type="int")
    
    client.add_index(idx1) # NB: Without this step, your indexes
                           #     won't be queryable.
    
    # Find all orders with an order number  < 200,000,000
    result = yield idx1.query("less_than", 200000000)
    print result
    
    reactor.stop()
    return

if __name__ == "__main__":
    d = defer.Deferred()
    d.addCallback(create_orders)
    d.addCallback(show_orders)
    
    reactor.callLater(0, d.callback, True)
    reactor.run()
