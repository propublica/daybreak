module Daybreak
  # Class for building out the table, you shouldn't need to access this
  # class directly. Readers are responsible for reading each record in
  # the file and yeilding the parsed records.
  class Reader
    # @param [String] file the file to read from
    def initialize(file)
      @file_name = file
    end

    # Read all values from the aof file.
    #
    # Right now this is really expensive, every call to read will
    # close and reread the whole db file, but since cross process
    # consistency is handled by the user, this should be fair warning.
    def read
      File.open(@file_name, 'r') do |fd|
        fd.binmode
        fd.advise(:sequential) if fd.respond_to? :advise
        while !fd.eof?
          yield Record.read(fd)
        end
      end
    end
  end
end
