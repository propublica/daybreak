module Daybreak
  # Daybreak::Journal handles background io, compaction and is the arbiter
  # of multiprocess safety
  # @api private
  class Journal < Queue
    attr_reader :size, :file

    def initialize(file, format, serializer, &block)
      super()
      @file, @format, @serializer, @emit = file, format, serializer, block
      open
      @worker = Thread.new(&method(:worker))
      @worker.priority = -1
      load
    end

    # Is the journal closed?
    def closed?
      @fd.closed?
    end

    # Clear the queue and close the file handler
    def close
      self << nil
      @worker.join
      @fd.close
      super
    end

    # Load new journal entries
    def load
      flush
      replay
    end

    # Lock the logfile across thread and process boundaries
    def lock
      # Flush everything to start with a clean state
      # and to protect the @locked variable
      flush

      with_flock(File::LOCK_EX) do
        replay
        result = yield
        flush
        result
      end
    end

    # Clear the database log and yield
    def clear
      flush
      with_tmpfile do |path, file|
        file.write(@format.header)
        file.close
        # Clear replaces the database file like a compactification does
        with_flock(File::LOCK_EX) do
          File.rename(path, @file)
        end
      end
      open
    end

    # Compact the logfile to represent the in-memory state
    def compact
      load
      with_tmpfile do |path, file|
        # Compactified database has the same size -> return
        return self if @pos == file.write(dump(yield, @format.header))
        with_flock(File::LOCK_EX) do
          # Database was replaced (cleared or compactified) in the meantime
          if @pos != nil
            # Append changed journal records if the database changed during compactification
            file.write(read)
            file.close
            File.rename(path, @file)
          end
        end
      end
      open
      replay
    end

    # Return byte size of journal
    def bytesize
      @fd.stat.size
    end

    private

    # Emit records as we parse them
    def replay
      buf = read
      until buf.empty?
        @emit.call(@format.parse(buf))
        @size += 1
      end
    end

    # Open or reopen file
    def open
      @fd.close if @fd
      @fd = File.open(@file, 'ab+')
      @fd.advise(:sequential) if @fd.respond_to? :advise
      stat = @fd.stat
      @inode = stat.ino
      write(@format.header) if stat.size == 0
      @pos = nil
    end

    # Read new file content
    def read
      with_flock(File::LOCK_SH) do
        # File was opened
        unless @pos
          @fd.pos = 0
          @format.read_header(@fd)
          @size = 0
          @emit.call(nil)
        else
          @fd.pos = @pos
        end
        buf = @fd.read
        @pos = @fd.pos
        buf
      end
    end

    # Return database dump as string
    def dump(records, dump = '')
      # each is faster than inject
      records.each do |record|
        record[1] = @serializer.dump(record.last)
        dump << @format.dump(record)
      end
      dump
    end

    # Worker thread
    def worker
      loop do
        case record = first
        when Hash
          # Write batch update
          write(dump(record))
          @size += record.size
        when nil
          pop
          break
        else
          # Write single record
          record[1] = @serializer.dump(record.last) if record.size > 1
          write(@format.dump(record))
          @size += 1
        end
        pop
      end
    rescue Exception => ex
      warn "Daybreak worker: #{ex.message}"
      @fd.close
    end

    # Write data to output stream and advance @pos
    def write(dump)
      with_flock(File::LOCK_EX) do
        @fd.write(dump)
        # Flush to make sure the file is really updated
        @fd.flush
      end
      @pos = @fd.pos if @pos && @fd.pos == @pos + dump.bytesize
    end

    # Block with file lock
    def with_flock(mode)
      return yield if @locked
      begin
        loop do
          # HACK: JRuby returns false if the process is already hold by the same process
          # see https://github.com/jruby/jruby/issues/496
          Thread.pass until @fd.flock(mode)
          # Check if database was replaced (cleared or compactified) in the meantime
          # break if not
          stat = @fd.stat
          break if stat.nlink > 0 && stat.ino == @inode
          open
        end
        @locked = true
        yield
      ensure
        @fd.flock(File::LOCK_UN)
        @locked = false
      end
    end

    # Open temporary file and pass it to the block
    def with_tmpfile
      path = [@file, $$.to_s(36), Thread.current.object_id.to_s(36)].join
      file = File.open(path, 'wb')
      yield(path, file)
    ensure
      file.close unless file.closed?
      File.unlink(path) if File.exists?(path)
    end
  end
end
