require 'socket'
require './RDB'

class RedisReplicatedHash
  attr_accessor :keys, :is_finished_bulk_load
  
  def connect(host, port)
    @socket = TCPSocket.open(host, port)
    @socket.puts("SYNC\r\n")
    
    reply_info = @socket.readline
    rdb_length = reply_info[1..-3].to_i # strip reply type identifier and CRCF
    rdb_data = @socket.read(rdb_length)
    
    rdb_data = StringIO.new(rdb_data)
    RDB::rdb_load(rdb_data) {|key, value| @keys[key] = value}
    @is_finished_bulk_load = true
  end
  
  def initialize(host = "localhost", port = 6379, blocking = false)
    @keys = {}
    @is_finished_bulk_load = false
    
    connect(host, port) if blocking
    
    @thread = Thread.new do
      begin
        connect(host, port) if !blocking
      
        # main processing loop
        while line = @socket.readline
          arguments = []
          argument_count = line[1..-2].to_i
  
          1.upto(argument_count) do
            line = @socket.readline
            argument_length = line[1..-2].to_i
            argument_data = @socket.read(argument_length)
            @socket.read(2) # CR-CF
            arguments << argument_data
          end
  
          if arguments[0] == 'set'
            key = arguments[1]
            value = arguments[2]
            @keys[key] = value
          end
        end
      rescue Exception
        puts "!!! Fatal error in redis-replicated-hash"
        p $!
      end
    end
  end
  
  def close
    @thread.exit if @thread.alive?
    @socket.close if !@socket.nil?
  end
end