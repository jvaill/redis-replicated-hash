An always up-to-date local memory hash for Ruby and Redis

## DESCRIPTION

Redis Replicated Hash is a simple Ruby library for synchronizing a local memory
hash against a Redis server.

To achieve this, Redis Replicated Hash functions as a basic Redis client that only
implements replication.

## USAGE

	require './redis-replicated-hash'
	
	replicated_hash = RedisReplicatedHash.new
	puts "Replicated hash is running, and probably synchronizing right now!"
	
	puts "Enter the key to retrieve:"
	while line = gets
	  puts "Retrieved value is:"
	  p replicated_hash.keys[line[0..-2]]
	end