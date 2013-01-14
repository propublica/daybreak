module Daybreak
  # Database format serializer and deserializer. You can create
  # your own implementations of this classes method and define
  # your own database format!
  # @api public
  class Format
    # Read database header from input stream
    # @param [#read] input the input stream
    def read_header(input)
      raise 'Not a Daybreak database' if input.read(MAGIC.bytesize) != MAGIC
      ver = input.read(2).unpack('n').first
      raise "Expected database version #{VERSION}, got #{ver}" if ver != VERSION
    end

    # Return database header as string
    def header
      MAGIC + [VERSION].pack('n')
    end

    # Serialize record and return string
    # @param [Array] record an array with [key, value] or [key] if the record is
    # deleted
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
    def parse(buf)
      key_size, value_size = buf[0, 8].unpack('NN')
      data = buf.slice!(0, 8 + key_size + (value_size == DELETE ? 0 : value_size))
      raise 'CRC mismatch' unless buf.slice!(0, 4) == crc32(data)
      value_size == DELETE ? [data[8, key_size]] : [data[8, key_size], data[8 + key_size, value_size]]
    end

    protected

    MAGIC = 'DAYBREAK'
    VERSION = 1
    DELETE = (1 << 32) - 1

    def crc32(s)
      [Zlib.crc32(s, 0)].pack('N')
    end
  end
end
