module Daybreak
  # Class for building out the table, you shouldn't need to access this
  # class directly.
  class Reader
    def initialize(file)
      @file_name = file
    end

    def close!
      @fd.close unless @fd.nil?
    end


    # Right now this is really expensive, every call to read will
    # close and reread the whole db file, but since cross process
    # consistency is handled by the user, this should be fair warning.
    def read(&blk)
      reopen!
      while !@fd.eof?
        blk.call(Record.read(@fd))
      end
    end

    private

    def reopen!
      close!
      open!
    end

    def open!
      @fd = File.open @file_name, 'r'
      @fd.binmode
      @fd.advise(:sequential) if @fd.respond_to? :advise
    end
  end
end
