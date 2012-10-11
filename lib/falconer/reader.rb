module Falconer
  # Single use class for building out the table
  class Reader
    class SingleUseError < Exception; end

    def initialize(file)
      @fd = File.open file, 'r'
      @fd.binmode
      @fd.advise(:sequential) if @fd.respond_to? :advise
    end

    def read(&blk)
      raise SingleUseError, "You can only use a Reader once" if @fd.closed?
      while !@fd.eof?
        blk.call(Record.read(@fd))
      end
      @fd.close
    end
  end
end
