require 'lzfruby'
require 'stringio'

module RDB
  REDIS_EXPIRETIME = 253
  REDIS_SELECTDB = 254
  REDIS_EOF = 255

  REDIS_STRING = 0
  REDIS_LIST = 1
  REDIS_SET = 2
  REDIS_ZSET = 3
  REDIS_HASH = 4

  REDIS_HASH_ZIPMAP = 9
  REDIS_LIST_ZIPLIST = 10
  REDIS_SET_INTSET = 11
  REDIS_ZSET_ZIPLIST = 12

  REDIS_RDB_6BITLEN = 0
  REDIS_RDB_14BITLEN = 1
  REDIS_RDB_32BITLEN = 2
  REDIS_RDB_ENCVAL = 3

  REDIS_RDB_ENC_INT8 = 0
  REDIS_RDB_ENC_INT16 = 1
  REDIS_RDB_ENC_INT32 = 2
  REDIS_RDB_ENC_LZF = 3

  R_PostInf = 1.0
  R_NegInf = -1.0
  R_Nan = 0.0

  def RDB.rdb_load_length(file)
    is_encoded = false
  
    buffer = file.readbyte
    type = (buffer & 0xC0) >> 6
    case type
    when REDIS_RDB_6BITLEN
      # read a 6 bit len
      return is_encoded, buffer & 0x3F
    when REDIS_RDB_ENCVAL
      # read a 6 bit len encoding type
      is_encoded = true
      return is_encoded, buffer & 0x3F
    when REDIS_RDB_14BITLEN
      # read a 14 bit len
      buffer2 = file.readbyte
      return is_encoded, ((buffer & 0x3F) << 8) | buffer2
    else
      # read a 32 bit len
      return is_encoded, file.read(4).unpack("N").first
    end
  end

  def RDB.rdb_load_integer_object(file, encoding_type, encode)
    case encoding_type
    when REDIS_RDB_ENC_INT8
      value = file.readbyte  
    when REDIS_RDB_ENC_INT16
      buffer = file.read(2)
      value = buffer.unpack("n").first
    when REDIS_RDB_ENC_INT32
      buffer = file.read(4)
      value = buffer.unpack("N").first
    else
      raise Exception.new("Unknown RDB integer encoding type")
    end
    return value.to_s
  end

  def RDB.rdb_load_type(file)
    file.readbyte
  end

  def RDB.rdb_load_time(file)
    file.read(4)
  end

  def RDB.rdb_load_lzf_string_object(file)
    begin
      is_encoded, compressed_length = rdb_load_length(file)
      is_encoded, length = rdb_load_length(file)
      compressed_data = file.read(compressed_length)
  
      decompressed_data = StringIO.new
      header = "ZV\001" + [compressed_length].pack("n") + [length].pack("n")
      compressed_data = StringIO.new(header + compressed_data)

      LZF.decompress(compressed_data, decompressed_data)
      return decompressed_data.string
    rescue
      return nil
    end
  end

  def RDB.rdb_generic_load_string_object(file, encode)
    is_encoded, length = rdb_load_length(file)
    if is_encoded
      case length
      when REDIS_RDB_ENC_INT8, REDIS_RDB_ENC_INT16, REDIS_RDB_ENC_INT32
        return rdb_load_integer_object(file, length, encode)
      when REDIS_RDB_ENC_LZF
        return rdb_load_lzf_string_object(file)
      else
        raise Exception.new("Unknown RDB encoding type")
      end
    end
  
    return file.read(length)
  end

  def RDB.rdb_load_string_object(file)
    return rdb_generic_load_string_object(file, 0)
  end

  def RDB.rdb_load_encoded_string_object(file)
    return rdb_generic_load_string_object(file, 1)
  end

  def RDB.rdb_load_double_value(file)
    length = file.readbyte
    case length
    when 255
      return R_NegInf
    when 254
      return R_PosInf
    when 253
      return R_Nan
    else
      buffer = file.read(length)
      return buffer.unpack("G")
    end
  end

  def RDB.rdb_load_object(type, file)
    case type
    when REDIS_STRING
      return rdb_load_encoded_string_object(file)
    when REDIS_LIST, REDIS_SET
      is_encoded, length = rdb_load_length(file)
      while length > 0
        rdb_load_encoded_string_object(file) 
        length = length - 1
      end
    when REDIS_ZSET
      is_encoded, length = rdb_load_length(file)
      while length > 0
        rdb_load_encoded_string_object(file)
        rdb_load_double_value(file)
        length = length - 1
      end
    when REDIS_HASH
      is_encoded, length = rdb_load_length(file)
      while length > 0
        rdb_load_encoded_string_object(file)
        rdb_load_encoded_string_object(file)
        length = length - 1
      end
    when REDIS_HASH_ZIPMAP, REDIS_LIST_ZIPLIST, REDIS_SET_INTSET, REDIS_ZSET_ZIPLIST
      rdb_load_string_object(file)
    else
      raise Exception.new("Unknown object type")
    end
  end

  def RDB.rdb_load(file)
    if file.read(5) != "REDIS"
      raise Exception.new("Wrong signature trying to load DB from file")
    end
    rdb_version = file.read(4).to_i
    if rdb_version < 1 || rdb_version > 2
      raise Exception.new("Can't handle RDB format version #{rdb_version.to_s}")
    end
  
    while true
      # read type
      type = rdb_load_type(file)
      if type == REDIS_EXPIRETIME
        expire_time = rdb_load_time(file)
        # we read the time so we need to read the object type again
        type = rdb_load_type(file)
      end
      break if type == REDIS_EOF
      # handle SELET DB opcode as a special case
      if type == REDIS_SELECTDB
        is_encoded, length = rdb_load_length(file)
        next
      end
      # read key
      key = rdb_load_string_object(file)
      value = rdb_load_object(type, file)
    
      yield key, value if !key.nil?
    end
  end
end