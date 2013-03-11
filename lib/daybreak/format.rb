module Daybreak
  # Database format serializer and deserializer. You can create
  # your own implementation of this class and define
  # your own database format!
  # @api public
  class Format
    # Read database header from input stream
    # @param [#read] input the input stream
    # @return void
    def read_header(input)
      raise 'Not a Daybreak database' if input.read(MAGIC.bytesize) != MAGIC
      ver = input.read(2).unpack('n').first
      raise "Expected database version #{VERSION}, got #{ver}" if ver != VERSION
    end

    # Return database header as string
    # @return [String] database file header
    def header
      MAGIC + [VERSION].pack('n')
    end

    # Serialize record and return string
    # @param [Array] record an array with [key, value] or [key] if the record is deleted
    # @return [String] serialized record
    def dump(record)
      data =
        if record.size == 1
          [record[0].bytesize, DELETE].pack('NN') << record[0]
        else
          [record[0].bytesize, record[1].bytesize].pack('NN') << record[0] << record[1]
        end
      data << crc32(data)
    end

    # Deserialize record from buffer
    # @param [String] buf the buffer to read from
    # @return [Array] deserialized record [key, value] or [key] if the record is deleted
    def parse(buf)
      size = buf.read(8)
      key_size, value_size = size.unpack('NN')
      data = buf.read(key_size + (value_size == DELETE ? 0 : value_size))
      raise 'CRC mismatch: your data might be corrupted!' unless buf.read(4) == crc32(size + data)
      value_size == DELETE ? [data[0, key_size]] : [data[0, key_size], data[key_size, value_size]]
    end

    protected

    # Magic string of the file header
    MAGIC = 'DAYBREAK'

    # Database file format version
    VERSION = 1

    # Special value size used for deleted records
    DELETE = (1 << 32) - 1

    # Compute crc32 of string
    # @param [String] s a string
    # @return [Fixnum]
    def crc32(s)
      [Zlib.crc32(s, 0)].pack('N')
    end
  end
end
