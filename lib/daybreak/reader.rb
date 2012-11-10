module Daybreak
  # Single use class for building out the table
  class Reader
    def initialize(file)
      @fd = File.open file, 'r'
      @fd.binmode
      @fd.advise(:sequential) if @fd.respond_to? :advise
    end

    def close!
      @fd.close
    end

    def read(&blk)
      while !@fd.eof?
        blk.call(Record.read(@fd))
      end
    end
  end
end
