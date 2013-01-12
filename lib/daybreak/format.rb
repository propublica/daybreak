module Daybreak
  class Format
    def initialize(serializer)
      @serializer = serializer
    end

    def read_header(input)
      raise 'Not a Daybreak database' if input.read(MAGIC.bytesize) != MAGIC
      ver = input.read(2).unpack('n').first
      raise "Expected database version #{VERSION}, got #{ver}" if ver != VERSION
    end

    def header
      @header ||= MAGIC + [VERSION].pack('n')
    end

    def serialize(record)
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

    MAGIC = 'DAYBREAK'
    VERSION = 1
    DELETE = (1 << 32) - 1

    def crc32(s)
      [Zlib.crc32(s, 0)].pack('N')
    end
  end
end
