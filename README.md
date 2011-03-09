# txRiakIdx: Twisted Riak client with transparent indexing #

## Overview ##

txRiakIdx is a superset of [txRiak](http://github/williamsjj/txriak) that implements transparent secondary indexing of keys. As long as you store your keys as valid JSON dictionaries, txRiakIdx can index them and query those indexes. This requires Riak 0.14.0 or newer since we leverage Key Filters to make index searching faster.

Let's say you ran a diner and you store every order in Riak under primary key of the current UNIX timestamp (order_1299648212). Inside your order keys you store a JSON dictionary representing the order:


	{"order_number" : 123456789,
	 "diner_name" : "Bobbie Jo Rickelbacker"}


Now what if you wanted to find every order "Bobbie Jo Rickelbacker" has placed in the diner? Without secondary indexes, you'd have to write a custom MapReduce job. But with txRiakIdx, you just define an index on `diner_name` and then finding all those orders is a simple as:

	name_index.query("eq", "Bobbie Jo Rickelbacker")

## License ##

BSD licensed. Check the headers of the source files for the specifics.

## Requirements & installing ##

* Python 2.6 or newer
* txRiak (you'll need this [fork](http://github/williamsjj/txriak) until Riak key filter support gets merged in.)
* Riak 0.14.0 or newer (for key filter support)

Installing is as simple as cloning this repo or grabbing it from the "Downloads" button and running:

	cd ./txriakidx
	python setup.py install

## How the indexing works ##

Indexes work by matching a key prefix and a bucket. For a key name like `order_12345` the key prefix would be `order`. We do this so you can target your indexes on individual types of keys even within a bucket. Every create, update or delete on a key that matches the bucket and key prefix triggers the indexes for that key to be created, updated or deleted respectively.

__You can index multiple fields in the key too. Just make a `RiakIndex` definition for each field you want indexed.__

An index entry is simply another Riak key in a specially-named bucket, with a specially-named key name. It's easier to show with an example. Let's say you have this Riak key:

__Bucket:___ `my_orders`

__Key Name:__ `order_12345`

__Value:__ `{"order_number" : 123456789, "diner_name" : "Bobbie Jo Rickelbacker"}`

And you define your index on the `diner_name` field of the JSON dictionary:

	idx = riakidx.RiakIndex(bucket="my_orders",
							key_prefix="order",
							indexed_field="diner_name",
							indexed_type="str")

That index definition tells txRiakIdx to create an index for the `diner_name` field in keys within the `my_orders` bucket that start with the `order` key prefix (txRiakIdx expects the key prefix and key name to be separated by \_). When you create the `order_12345` order key, here's the index key that txRiakIdx creates:

__Bucket:__ `idx=my_orders=order=diner_name`

__Key Name:__ `order_12345/Bobbie Jo Rickelbacker`

__Value:__ (empty)


So when decide you want to see all the keys with `diner_name` of "Bobbie Jo Rickelbacker", txRiakIdx tells Riak (using key filters) to find all of the keys in `idx=my_orders=order=diner_name` where "Bobbie Jo Rickelbacker" is found after the `/` in the index key name. But all your program sees back from the `.query()` command is:

	[("my_orders", "order_12345", "Bobbie Jo Rickelbacker")]

In otherwords, you get back a list of the original data keys (and their buckets) whose "diner\_name" field is "Bobbie Jo Rickelbacker".

You might have noticed that the RiakIndex definition takes an argument called `indexed_type`. This can be `str`, `unicode`, `int`, `bool` or `float`. This allows txRiakIdx to perform searches like: show me all of the order numbers smaller than 2000. txRiakIdx will transparently convert the integer to a string when storing the index key, and will then tell the key filter to treat the value like a integer when you query.

The `RiakIndex.query()` function accepts any Riak key filter predicate function as a comparison operator. A list of the predicate function names is here: [http://wiki.basho.com/Key-Filters.html#Predicate-functions](http://wiki.basho.com/Key-Filters.html#Predicate-functions)

## Encoding values ##

Since we're using the Riak REST/HTTP API, all of our bucket and key names are URL encoded. So `idx=my_orders=order=diner_name` becomes `idx%3Dmy_orders=order%3Ddiner_name`, and `order_12345/joe` becomes `order_123456%2Fjoe`. However, we could run into the issue where the value being indexed contains a `/` character which would confuse Riak's key filter tokenizer. So first we URL encode the value being indexed, and then concatenate it to the key name and finally URL encode the entire key name.

So `order_12345/Bobbie Jo Rickelbacker` becomes `order_12345/Bobbie%2520Jo%2520Rickelbacker`. 

To unpack an index key name:

1. URL decode the entire key name.
2. Split the decoded key name on `/` to get the data key name and the indexed value.
3. URL decode the indexed value.


