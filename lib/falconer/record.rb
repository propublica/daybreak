module Falconer
  class Record
    class UnnacceptableDataError < Exception; end
    class CorruptDataError < Exception; end
    include Locking

    attr_accessor :key, :data

    def initialize(key = nil, data = nil)
      @key  = key
      @data = data
    end

    def read(io)
      lock io do
        @key  = read_bytes(io)
        @data = read_bytes(io)
        crc   = io.read(4)
        raise CorruptDataError, "CRC mismatch #{crc} should be #{crc_string}" unless crc == crc_string
      end
      self
    end

    def self.read(io)
      new.read(io)
    end

    def representation
      raise UnnacceptableDataError, "key and data must be defined" if @key.nil? || @data.nil?
      byte_string + crc_string
    end

    private

    def byte_string
      @byte_string ||= part(@key) + part(@data)
    end

    def crc_string
      Array(Zlib.crc32(byte_string, 0)).pack('N')
    end

    def read_bytes(io)
      raw = io.read(4)
      length = raw.unpack('N')[0]
      io.read(length)
    end

    def part(data)
      Array(data.bytesize).pack('N') + data
    end
  end
end
