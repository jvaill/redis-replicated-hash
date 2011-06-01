require './redis-replicated-hash'

replicated_hash = RedisReplicatedHash.new
puts "Replicated hash is running, and probably synchronizing right now!"

puts "Enter the key to retrieve:"
while line = gets
  puts "Retrieved value is:"
  p replicated_hash.keys[line[0..-2]]
end