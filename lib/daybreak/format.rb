module Daybreak
  class Format
    VERSION = 1

    def initialize(serializer)
      @serializer = serializer
    end

    def serialize(record)
      raise 'Key must be a string' unless String === record[0]
      data =
        if record.size == 1
          [record[0].bytesize, DELETE].pack('NN') << record[0]
        else
          value = @serializer.dump(record[1])
          [record[0].bytesize, value.bytesize].pack('NN') << record[0] << value
        end
      data << crc32(data)
    end

    def deserialize(buf)
      key_size, value_size = buf[0, 8].unpack('NN')
      data = buf.slice!(0, 8 + key_size + (value_size == DELETE ? 0 : value_size))
      raise 'CRC mismatch' unless buf.slice!(0, 4) == crc32(data)
      if value_size == DELETE
        [data[8, key_size]]
      else
        [data[8, key_size], @serializer.load(data[8 + key_size, value_size])]
      end
    end

    protected

    DELETE = (1 << 32) - 1

    def crc32(s)
      [Zlib.crc32(s, 0)].pack('N')
    end
  end
end
