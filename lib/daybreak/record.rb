module Daybreak
  # Records define how data is serialized and read from disk.
  class Record
    # Thrown when either key or data is missing
    class UnnacceptableDataError < Exception; end

    # Thrown when there is a CRC mismatch between the data from the disk
    # and what was written to disk previously.
    class CorruptDataError < Exception; end
    include Locking

    attr_accessor :key, :data

    def initialize(key = nil, data = nil)
      @key  = key
      @data = data
    end

    # Read a record from an open io source, check the CRC, and set @key and @data
    # @param [#read] io an IO instance to read from
    def read(io)
      lock io do
        @key  = read_bytes(io)
        @data = read_bytes(io)
        crc   = io.read(4)
        raise CorruptDataError, "CRC mismatch #{crc} should be #{crc_string}" unless crc == crc_string
      end
      self
    end

    # The serialized representation of the key value pair plus the CRC
    # @return [String]
    def representation
      raise UnnacceptableDataError, "key and data must be defined" if @key.nil? || @data.nil?
      byte_string + crc_string
    end

    # Create a new record to read from IO.
    # @param [#read] io an IO instance to read from
    def self.read(io)
      new.read(io)
    end

    private

    def byte_string
      @byte_string ||= part(@key) + part(@data)
    end

    def crc_string
      [Zlib.crc32(byte_string, 0)].pack('N')
    end

    def read_bytes(io)
      raw = io.read(4)
      length = raw.unpack('N')[0]
      io.read(length)
    end

    def part(data)
      [data.bytesize].pack('N') + data
    end
  end
end
