module Daybreak
  # Records define how data is serialized and read from disk.
  class Record
    # Thrown when either key or data is missing
    class UnnacceptableDataError < Exception; end

    # Thrown when there is a CRC mismatch between the data from the disk
    # and what was written to disk previously.
    class CorruptDataError < Exception; end
    include Locking

    # The mask a record uses to check for deletion.
    DELETION_MASK = (1 << 32)

    attr_accessor :key, :data

    def initialize(key = nil, data = nil, deleted = false)
      @key  = key
      @data = data
      @deleted = deleted
    end

    # Read a record from an open io source, check the CRC, and set <tt>@key</tt>
    # and <tt>@data</tt>.
    # @param [#read] io an IO instance to read from
    def read(io)
      lock io do
        @key  = read_key(io)
        @data = read_data(io)
        crc   = io.read(4)
        raise CorruptDataError, "CRC mismatch #{crc} should be #{crc_string}" unless crc == crc_string
      end
      self
    end

    # The serialized representation of the key value pair plus the CRC.
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

    def deleted?
      @deleted == 1
    end

    private

    def byte_string
      @byte_string ||= part(@key) + part(@data)
    end

    def crc_string
      [Zlib.crc32(byte_string, 0)].pack('N')
    end

    def read_data(io)
      io.read read32(io)
    end

    def read_key(io)
      masked   = read32 io
      @deleted = masked | DELETION_MASK
      length   = masked >> 1
      io.read length
    end

    def read32(io)
      raw = io.read(4)
      raw.unpack('N')[0]
    end

    def part(data)
      [data.bytesize].pack('N') + data
    end
  end
end
