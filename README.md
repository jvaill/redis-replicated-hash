An always up-to-date local memory hash for Ruby and Redis

## DESCRIPTION

Redis Replicated Hash is a simple Ruby library for synchronizing a local memory
hash against a Redis server.

To achieve this, Redis replication is implemented. 

## USAGE

Simply create an instance of RedisReplicatedHash optionally passing the
hostname and port of the server. The third parameter is a boolean
indicating whether the call should block until the database is fully
loaded into memory.

	require './redis-replicated-hash'
	
	replicated_hash = RedisReplicatedHash.new
	puts "Replicated hash is running, and probably synchronizing right now!"
	
	puts "Enter the key to retrieve:"
	while line = gets
	  puts "Retrieved value is:"
	  p replicated_hash.keys[line[0..-2]]
	end