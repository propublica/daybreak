module Daybreak
  # Records define how data is serialized and read from disk.
  module Record
    # Thrown when either key or data is missing
    class UnnacceptableDataError < Exception; end

    # Thrown when there is a CRC mismatch between the data from the disk
    # and what was written to disk previously.
    class CorruptDataError < Exception; end

    extend self

    # The mask a record uses to check for deletion.
    DELETION_MASK = 1 << 31

    # Read a record from an open io source, check the CRC, and set <tt>@key</tt>
    # and <tt>@data</tt>.
    # @param [#read] io an IO instance to read from

    # The serialized representation of the key value pair plus the CRC.
    # @return [String]
    def serialize(record)
      raise UnnacceptableDataError, 'key and data must be defined' unless record[0] && record[1]
      s = key_data_string(record)
      s << crc_string(s)
    end

    # Create a new record to read from IO.
    # @param [#read] io an IO instance to read from
    def deserialize(buf)
      record = []
      masked = read32(buf)
      # Read the record's key bytes
      record << buf.slice!(0, masked & (DELETION_MASK - 1)) <<
        # Read the record's value bytes
        buf.slice!(0, read32(buf)) <<
        # Set the deletion flag
        ((masked & DELETION_MASK) != 0)
      raise CorruptDataError, 'CRC mismatch' unless buf.slice!(0, 4) == crc_string(key_data_string(record))
      record
    end

    private

    # Return the deletion flag plus two length prefixed cells
    def key_data_string(record)
      part(record[0], record[0].bytesize + (record[2] ? DELETION_MASK : 0)) << part(record[1], record[1].bytesize)
    end

    def crc_string(s)
      [Zlib.crc32(s, 0)].pack('N')
    end

    def part(data, length)
      [length].pack('N') << data
    end

    def read32(buf)
      buf.slice!(0, 4).unpack('N')[0]
    end
  end
end
