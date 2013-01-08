module Daybreak
  # Class for building out the table, you shouldn't need to access this
  # class directly. Readers are responsible for reading each record in
  # the file and yeilding the parsed records.
  class Reader
    # @param [String] file the file to read from
    def initialize(file)
      @file_name = file
    end

    # Close the Reader's file descriptor.
    def close!
      @fd.close unless @fd.nil? || !@fd.closed?
    end

    # Read all values from the aof file.
    #
    # Right now this is really expensive, every call to read will
    # close and reread the whole db file, but since cross process
    # consistency is handled by the user, this should be fair warning.
    def read
      open!
      while !@fd.eof?
        yield Record.read(@fd)
      end
    ensure
      close!
    end

    private

    def open!
      @fd = File.open @file_name, 'r'
      @fd.binmode
      @fd.advise(:sequential) if @fd.respond_to? :advise
    end
  end
end
